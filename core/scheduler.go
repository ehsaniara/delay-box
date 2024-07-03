package core

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ehsaniara/scheduler/config"
	"github.com/ehsaniara/scheduler/interfaces"
	"github.com/ehsaniara/scheduler/kafka"
	_pb "github.com/ehsaniara/scheduler/proto"
	"github.com/gogo/protobuf/proto"
	"github.com/redis/go-redis/v9"
	"log"
	"strconv"
	"sync"
	"time"
)

type Scheduler interface {
	Dispatcher(message *sarama.ConsumerMessage)
	Stop()
	Get() []_pb.Task
}

type scheduler struct {
	quit        chan struct{}
	redisClient interfaces.RedisDBClient
	producer    kafka.SyncProducer
	ctx         context.Context
	config      *config.Config
}

func NewScheduler(ctx context.Context, redisClient interfaces.RedisDBClient, producer kafka.SyncProducer, config *config.Config) Scheduler {
	s := &scheduler{
		quit:        make(chan struct{}),
		redisClient: redisClient,
		producer:    producer,
		ctx:         ctx,
		config:      config,
	}

	log.Printf("✔️ Scheduler is waiting to start..")
	s.run()
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
	s.setNewTask(executionTimestamp, &task)
}

func (s *scheduler) setNewTask(executionTimestamp float64, task *_pb.Task) {

	marshal, err := proto.Marshal(task)
	if err != nil {
		fmt.Println("❌  Error: marshal", err)
		return
	}

	s.redisClient.ZAdd(s.ctx, s.config.Storage.SchedulerKeyName, redis.Z{
		Score:  executionTimestamp,
		Member: marshal,
	})

	log.Print("--------- task Added ----------")

}

// language=lua
const FetchAndRemoveScript = `
local currentTime = tonumber(ARGV[1])
local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', currentTime)
if #tasks > 0 then
	redis.call('ZREM', KEYS[1], unpack(tasks))
end
return tasks
`
const ListOfAllTaskScriptThenRemove = `
    local tasks = redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[2])
    if ARGV[3] == "delete" then
        for i, task in ipairs(tasks) do
            redis.call("ZREM", KEYS[1], task)
        end
    end
    return tasks
    `

// language=lua
const ListOfAllTaskScript = `
    return redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[2])
    `

func (s *scheduler) run() {
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
				//log.Println(".")
				unixMilli := time.Now().UnixMilli()
				//log.Println(unixMilli)
				cmd := s.redisClient.Eval(s.ctx, FetchAndRemoveScript, []string{s.config.Storage.SchedulerKeyName}, float64(unixMilli))

				//it should call once to release the wg.Wait()
				once.Do(func() {
					wg.Done()
				})
				if s.ctx.Err() != nil {
					continue
				}

				if cmd == nil {
					continue
				}
				if cmd.Err() != nil {
					continue
				}

				result, err := cmd.Result()
				if err != nil {
					log.Panicf("❌  Run err: %v", err)
				}

				if result == nil {
					continue
				}

				// Process the result
				dueTasks := result.([]interface{})
				s.executeDueTasks(dueTasks)
			}
		}
	}(&wg)
	wg.Wait()
}

func (s *scheduler) executeDueTasks(dueTasks []interface{}) {
	for _, task := range dueTasks {
		taskStr, okStr := task.(string)
		if !okStr {
			taskBytes, okB := task.([]byte)
			if !okB {
				fmt.Println("Error: unexpected type for task", task)
				continue
			}
			taskStr = string(taskBytes)
		}
		go s.processTask([]byte(taskStr))
	}
}

func (s *scheduler) processTask(payload []byte) {
	log.Print("--------- task processed ----------")

	message := &sarama.ProducerMessage{
		Topic: s.config.Kafka.TaskExecutionTopic,
		Value: sarama.ByteEncoder(payload),
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

func (s *scheduler) Get() []_pb.Task {
	log.Println("Get called")
	//maxTimestamp := fmt.Sprintf("%d", time.Now().Add(10*time.Second).UnixMilli())
	//return redisClient.Eval(ctx, listOfAllTaskScript, []string{core.SchedulerKeyName}, "0", maxTimestamp, "delete").Result()
	return nil
}
