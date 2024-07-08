package core

import (
	"context"
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
	PublishNewTask(task *_pb.Task)
	Dispatcher(message *sarama.ConsumerMessage)
	Stop()
}

type scheduler struct {
	quit     chan struct{}
	storage  storage.TaskStorage
	producer kafka.SyncProducer
	ctx      context.Context
	config   *config.Config
}

func NewScheduler(ctx context.Context, storage storage.TaskStorage, producer kafka.SyncProducer, config *config.Config) Scheduler {
	s := &scheduler{
		quit:     make(chan struct{}),
		storage:  storage,
		producer: producer,
		ctx:      ctx,
		config:   config,
	}

	log.Printf("✔️ Scheduler is waiting to start..")
	s.Serve(ctx)
	log.Printf("✔️ Scheduler is started.")

	return s
}

func (s *scheduler) PublishNewTask(task *_pb.Task) {
	log.Print("--------- publish task ----------")

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

	partition, offset, err := s.producer.SendMessage(message)
	if err != nil {
		log.Panicf("❌  Producer err: %v", err)
		return
	}
	log.Printf("p:%d o:%d", partition, offset)
}

func (s *scheduler) Dispatcher(message *sarama.ConsumerMessage) {
	log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s\n", string(message.Value), message.Timestamp, message.Topic)

	if message.Headers == nil {
		log.Printf("Headers are missing")
		return
	}

	header := make(map[string][]byte)

	// if executionTimestamp missing, it will be run right away
	hasExecutionTimestamp := false
	for _, h := range message.Headers {
		header[string(h.Key)] = h.Value

		if string(h.Key) == config.ExecutionTimestamp {
			hasExecutionTimestamp = true
		}
	}

	if !hasExecutionTimestamp {
		unixMilli := time.Now().UnixMilli()
		log.Printf("executionTimestamp is missing the message h so it's : %v", unixMilli)
		header[config.ExecutionTimestamp] = []byte(strconv.FormatInt(unixMilli, 10))
	}

	for _, h := range message.Headers {
		header[string(h.Key)] = h.Value
	}

	task := _pb.Task{
		Header: header,
		Pyload: message.Value,
	}
	s.storage.SetNewTask(s.ctx, &task)
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
				time.Sleep(200 * time.Millisecond) // Poll every 200 millisecond
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
	log.Print("--------- task processed ----------")

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
