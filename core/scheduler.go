package core

// You only need **one** of these per package!
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ehsaniara/delay-box/config"
	"github.com/ehsaniara/delay-box/kafka"
	_pb "github.com/ehsaniara/delay-box/proto"
	"github.com/ehsaniara/delay-box/storage"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

//counterfeiter:generate . Scheduler
type Scheduler interface {
	SetUpSubscriber(ctx context.Context)
	GetAllTasksPagination(ctx context.Context, offset, limit int32) []*_pb.Task
	Schedule(pyload string, header map[string]string) error
	Subscribe(ctx context.Context)
	Stop()
}

type scheduler struct {
	quit                         chan struct{}
	storage                      storage.TaskStorage
	producer                     kafka.SyncProducer
	ctx                          context.Context
	config                       *config.Config
	convertParameterToTaskHeader ConvertParameterToTaskHeaderFn
	pubSub                       *redis.PubSub
	dispatchSchedulerConsumer    kafka.Consumer
}

func NewScheduler(ctx context.Context, storage storage.TaskStorage, producer kafka.SyncProducer, config *config.Config) Scheduler {

	log.Printf("✔️ Scheduler is waiting to start..")

	s := &scheduler{
		quit:                         make(chan struct{}),
		storage:                      storage,
		producer:                     producer,
		ctx:                          ctx,
		config:                       config,
		convertParameterToTaskHeader: convertParameterToTaskHeader,
	}

	s.Serve(ctx)

	return s
}

func (s *scheduler) SetUpSubscriber(ctx context.Context) {
	//start all consumers 1
	if s.config.Kafka.Enabled {
		consumerGroup1 := kafka.NewConsumerGroup(s.config)
		dispatchSchedulerConsumer := kafka.NewConsumer(
			strings.Split(s.config.Kafka.SchedulerTopic, `,`),
			consumerGroup1,
			s.KafkaDispatcher,
		)
		dispatchSchedulerConsumer.Start(ctx)
		s.dispatchSchedulerConsumer = dispatchSchedulerConsumer
	} else {
		go s.Subscribe(ctx)
	}
	log.Printf("✔️ Scheduler is started.")
}

// GetAllTasksPagination gat all tasks using ListOfAllTaskScript
func (s *scheduler) GetAllTasksPagination(ctx context.Context, offset, limit int32) []*_pb.Task {
	return s.storage.GetAllTasksPagination(ctx, offset, limit)
}

func (s *scheduler) Schedule(pyload string, header map[string]string) error {
	byteArray, err := base64.StdEncoding.DecodeString(pyload)
	if err != nil {
		fmt.Printf("Error decoding base64 string: %v\n", err)
		return err
	}

	taskType, ok := header["taskType"]
	if !ok {
		if taskType != "PUB_SUB" && !s.config.WorkerEnable {
			return fmt.Errorf("worker is not enable for none PUB_SUB taskTypes")
		}
	}

	// assigning to header
	var headerMap map[string][]byte = nil
	if header != nil && len(header) > 0 {
		headerMap = s.convertParameterToTaskHeader(header)
	} else {
		log.Println("no header")
	}

	pbTask := &_pb.Task{
		Header: headerMap,
		Pyload: byteArray,
	}

	// enable to use kafka or direct write to redis
	// recommended to enable kafka in high write traffic
	if s.config.Kafka.Enabled {
		return s.PublishNewTaskToKafka(pbTask)
	} else {
		return s.PublishNewTaskToRedis(pbTask)
	}
}

// PublishNewTaskToRedis to publish a task into Redis SchedulerChanel
func (s *scheduler) PublishNewTaskToRedis(task *_pb.Task) error {
	marshal, err := proto.Marshal(task)
	if err != nil {
		return err
	}

	publish := s.storage.Publish(s.ctx, s.config.Storage.SchedulerChanel, marshal)
	if err = publish.Err(); err != nil {
		return err
	}
	return nil
}

// PublishNewTaskToKafka tp publish new task into SchedulerTopic topic so later on it will be stored in redis.
// We need this to have no slowness in receiving tasks and make it resilient using Kafka
func (s *scheduler) PublishNewTaskToKafka(task *_pb.Task) error {
	headers := make([]sarama.RecordHeader, 0)
	for k, v := range task.Header {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: v,
		})
	}

	message := &sarama.ProducerMessage{
		Topic:   s.config.Kafka.SchedulerTopic,
		Headers: headers,
		Value:   sarama.ByteEncoder(task.Pyload),
	}

	_, _, err := s.producer.SendMessage(message)
	if err != nil {
		return err
	}
	return nil
}

// KafkaDispatcher On system convenience it Tasks are consumed from Kafka topic and stored in Redis.
func (s *scheduler) KafkaDispatcher(message *sarama.ConsumerMessage) {
	log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s\n", string(message.Value), message.Timestamp, message.Topic)

	if message.Headers == nil {
		log.Printf("Headers are missing")
		return
	}

	header := make(map[string][]byte)

	// if executionTimestamp missing, it will be run right away
	executionTimestamp := getExecutionTimestamp(message)

	unixMilli := time.Now().UnixMilli()
	if executionTimestamp == 0 {
		log.Printf("executionTimestamp is missing the message header so it's: %v", unixMilli)
		header[config.ExecutionTimestamp] = []byte(strconv.FormatInt(unixMilli, 10))
	}

	for _, h := range message.Headers {
		header[string(h.Key)] = h.Value
	}

	log.Printf("scheduled task to be run in: %v ms\n", executionTimestamp-unixMilli)

	task := _pb.Task{
		Header: header,
		Pyload: message.Value,
		Status: _pb.Task_SCHEDULED,
	}
	s.storage.SetNewTask(s.ctx, &task)
}

func getExecutionTimestamp(message *sarama.ConsumerMessage) int64 {
	if message.Headers == nil || len(message.Headers) == 0 {
		return 0
	}

	for _, h := range message.Headers {

		// already has ExecutionTimestamp
		if string(h.Key) == config.ExecutionTimestamp {
			str := string(h.Value)
			convert, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				log.Printf("ExecutionTimestamp convert err: %v value:%v\n", err, str)
				return 0
			}

			return convert
		}
	}
	return 0
}

func (s *scheduler) Subscribe(ctx context.Context) {
	pubsub := s.storage.Subscribe(ctx, s.config.Storage.SchedulerChanel)

	// Wait for confirmation that subscription is created

	if _, err := pubsub.Receive(ctx); err != nil {
		fmt.Println("❌  Receive")
		return
	}

	s.pubSub = pubsub
	ch := pubsub.ChannelWithSubscriptions()

	for {
		select {
		case msgInterface := <-ch:
			switch msg := msgInterface.(type) {
			case *redis.Message:
				var task _pb.Task
				// Convert item to []byte
				if err := proto.Unmarshal([]byte(msg.Payload), &task); err != nil {
					fmt.Printf("❌  Failed to unmarshal protobuf: %v\n", err)
					continue
				}
				s.storage.SetNewTask(ctx, &task)
			case *redis.Subscription:
				fmt.Printf("Subscription: kind=%s, channel=%s, count=%d\n", msg.Kind, msg.Channel, msg.Count)
			default:
				fmt.Println("Unknown message type")
			}

		case <-s.quit:
			log.Println("👍 Subscribe stopped by signal.")
			return
		}
	}
}

func (s *scheduler) Serve(ctx context.Context) {
	var once sync.Once
	var wg sync.WaitGroup

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		for {
			select {
			case <-s.quit:
				log.Println("Scheduler stopped by signal.")
				return
			default:
				var f = time.Duration(s.config.Frequency)
				time.Sleep(f * time.Millisecond) // Poll every 100 millisecond
				//it should call once to release the wg.Wait()
				once.Do(func() {
					wg.Done()
				})

				tasks := s.storage.FetchAndRemoveDueTasks(ctx)
				s.executeDueTasks(tasks)
			}
		}
	}(&wg)
	wg.Wait()
}

func (s *scheduler) executeDueTasks(dueTasks []*_pb.Task) {
	if len(dueTasks) == 0 {
		return
	}
	for _, task := range dueTasks {
		if s.config.Kafka.Enabled {
			go s.processTaskWithKafka(task)
		} else {
			go s.processTaskWithRedis(task)
		}
	}
}

func (s *scheduler) processTaskWithKafka(task *_pb.Task) {
	for k, bytes := range task.Header {
		if k == config.ExecutionTimestamp {
			str := string(bytes)
			convert, _ := strconv.ParseInt(str, 10, 64)
			unixMilli := time.Now().UnixMilli()
			log.Printf("--------- task processed ----------> %d", convert-unixMilli)
		}
	}

	marshal, err := proto.Marshal(task)
	if err != nil {
		fmt.Println("❌  Error: marshal", err)
		return
	}

	message := &sarama.ProducerMessage{
		Topic: s.config.Kafka.TaskExecutionTopic,
		Value: sarama.ByteEncoder(marshal),
	}

	partition, offset, err := s.producer.SendMessage(message)
	if err != nil {
		// be added in DLQ
		log.Panicf("❌  Producer err: %v", err)
		return
	}
	log.Printf("p:%d o:%d", partition, offset)
}

func (s *scheduler) processTaskWithRedis(task *_pb.Task) {
	marshal, err := proto.Marshal(task)
	if err != nil {
		log.Panicf("❌  Publish Marshal err: %v", err)
	}

	publish := s.storage.Publish(s.ctx, s.config.Storage.TaskExecutionChanel, marshal)

	if err = publish.Err(); err != nil {
		log.Panicf("❌  Publish err: %v", err)
	}
}

func (s *scheduler) Stop() {
	log.Println("⏳  Scheduler Stopping...")
	close(s.quit)

	if s.config.Kafka.Enabled {
		s.dispatchSchedulerConsumer.Stop()
	} else {

		if err := s.pubSub.Close(); err != nil {
			log.Printf("❌  pubsub err: %v", err)
		}
	}
	log.Println("👍 Scheduler Stopped.")
}

// ConvertParameterToTaskHeaderFn use as injection
type ConvertParameterToTaskHeaderFn func(header map[string]string) map[string][]byte

// convertParameterToTaskHeader return map[string][]byte
func convertParameterToTaskHeader(header map[string]string) map[string][]byte {
	var pbHeader = make(map[string][]byte)
	for k, v := range header {
		pbHeader[k] = []byte(v)
	}
	return pbHeader
}
