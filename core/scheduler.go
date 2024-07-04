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
	Dispatcher(message *sarama.ConsumerMessage)
	Stop()
	GetAllTasks(ctx context.Context, offset, limit int32) []*_pb.Task
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

func (s *scheduler) Dispatcher(message *sarama.ConsumerMessage) {
	log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s\n", string(message.Value), message.Timestamp, message.Topic)

	if message.Headers == nil {
		return
		//message.Headers = make([]sarama.RecordHeader, 0, 1)
	}

	// if executionTimestamp missing, it will be run right away
	var executionTimestamp float64 = 0
	for _, header := range message.Headers {
		if header != nil && string(header.Key) == "executionTimestamp" {
			floatVal, _ := strconv.ParseFloat(string(header.Value), 64)
			executionTimestamp = floatVal
		}
	}
	if executionTimestamp == 0 {
		unixMilli := time.Now().UnixMilli()
		log.Printf("executionTimestamp is missing the message header so it's : %v", unixMilli)
		executionTimestamp = float64(unixMilli)
	}

	task := _pb.Task{
		ExecutionTimestamp: executionTimestamp,
		Header:             make(map[string][]byte),
		Pyload:             message.Value,
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

	message := &sarama.ProducerMessage{
		Topic: s.config.Kafka.TaskExecutionTopic,
		Value: sarama.ByteEncoder(task.Pyload),
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

func (s *scheduler) GetAllTasks(ctx context.Context, offset, limit int32) []*_pb.Task {
	log.Println("Get called")
	//maxTimestamp := fmt.Sprintf("%d", time.Now().Add(10*time.Second).UnixMilli())
	//return redisClient.Eval(ctx, listOfAllTaskScript, []string{core.SchedulerKeyName}, "0", maxTimestamp, "delete").Result()
	return nil
}
