package core

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ehsaniara/scheduler/config"
	"github.com/ehsaniara/scheduler/kafka"
	_pb "github.com/ehsaniara/scheduler/proto"
	"github.com/ehsaniara/scheduler/storage"
	"log"
	"strconv"
	"sync"
	"time"
)

type Scheduler interface {
	GetAllTasksPagination(ctx context.Context, offset, limit int32) []*_pb.Task
	Schedule(pyload string, header map[string]string) error
	PublishNewTaskToKafka(task *_pb.Task)
	Dispatcher(message *sarama.ConsumerMessage)
	Stop()
}

type scheduler struct {
	quit                         chan struct{}
	storage                      storage.TaskStorage
	producer                     kafka.SyncProducer
	ctx                          context.Context
	config                       *config.Config
	convertParameterToTaskHeader ConvertParameterToTaskHeaderFn
}

func NewScheduler(ctx context.Context, storage storage.TaskStorage, producer kafka.SyncProducer, config *config.Config) Scheduler {
	s := &scheduler{
		quit:                         make(chan struct{}),
		storage:                      storage,
		producer:                     producer,
		ctx:                          ctx,
		config:                       config,
		convertParameterToTaskHeader: convertParameterToTaskHeader,
	}

	log.Printf("✔️ Scheduler is waiting to start..")
	s.Serve(ctx)
	log.Printf("✔️ Scheduler is started.")

	return s
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
		s.PublishNewTaskToKafka(pbTask)
	} else {
		// not implemented
	}

	return nil
}

// PublishNewTaskToKafka tp publish new task into SchedulerTopic topic so later on it will be stored in redis.
func (s *scheduler) PublishNewTaskToKafka(task *_pb.Task) {
	log.Print("--------- publish new task ----------")

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
		log.Panicf("❌  Producer err: %v", err)
		return
	}
}

func (s *scheduler) Dispatcher(message *sarama.ConsumerMessage) {
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
		go s.processTask(task)
	}
}

func (s *scheduler) processTask(task *_pb.Task) {
	for k, bytes := range task.Header {
		if k == config.ExecutionTimestamp {
			str := string(bytes)
			convert, _ := strconv.ParseInt(str, 10, 64)
			unixMilli := time.Now().UnixMilli()
			log.Printf("--------- task processed ----------> %d", convert-unixMilli)
		}
	}

	task.Status = _pb.Task_PROCESSING
	headers := make([]sarama.RecordHeader, 0)
	for k, v := range task.Header {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: v,
		})
	}

	message := &sarama.ProducerMessage{
		Topic:   s.config.Kafka.TaskExecutionTopic,
		Value:   sarama.ByteEncoder(task.Pyload),
		Headers: headers,
	}

	partition, offset, err := s.producer.SendMessage(message)
	if err != nil {
		// be added in DLQ
		task.Status = _pb.Task_FAILED
		log.Panicf("❌  Producer err: %v", err)
		return
	}
	log.Printf("p:%d o:%d", partition, offset)
}

func (s *scheduler) Stop() {
	log.Println("⏳  Scheduler Stopping...")
	close(s.quit)
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
