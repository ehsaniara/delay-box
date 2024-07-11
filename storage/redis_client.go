package storage

// You only need **one** of these per package!
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"context"
	"fmt"
	"github.com/ehsaniara/scheduler/config"
	"github.com/ehsaniara/scheduler/interfaces"
	_pb "github.com/ehsaniara/scheduler/proto"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"log"
	"strconv"
	"time"
)

// indicator to see if *redis.Client are exist
var _ interfaces.RedisDBClient = (*redis.Client)(nil)

// NewRedisClient returns RedisDBClient to use in application, and close func
// rdb parameter is to test this file and pass mocks
func NewRedisClient(ctx context.Context, config *config.Config) (TaskStorage, func()) {

	if len(config.Storage.RedisHost) == 0 {
		log.Fatal("redisHost is missing")
	}

	client := &taskRedisClient{
		config:             config,
		convertByteToTasks: ConvertByteToTasks,
	}

	client.SetUp(ctx, config.Storage.RedisHost, config.Storage.RedisPass, config.Storage.RedisDb)

	return client, func() {
		log.Printf("⏳ Redis connection pool Closing...\n")
		err := client.rdb.Close()
		if err != nil {
			log.Printf("Error Close taskRedisClient:%v \n", err)
			return
		}
		log.Printf("👍 Redis connection pool Closed!\n")
	}
}

type taskRedisClient struct {
	rdb                interfaces.RedisDBClient
	config             *config.Config
	convertByteToTasks ConvertByteToTasksFn
}

func (c *taskRedisClient) SetUp(ctx context.Context, redisHost, redisPass string, database int) {

	if len(redisHost) == 0 {
		log.Fatal("redisHost is missing")
	}

	log.Printf("⏳ Redis Setting up... \n")
	c.rdb = redis.NewClient(&redis.Options{
		Addr:     redisHost,
		Password: redisPass, // secret
		DB:       database,  // use default DB
		Network:  "tcp",
	})
	log.Printf("✔️ Redis Started on Host: %v\n", c.rdb)
	pong, err := c.rdb.Ping(ctx).Result()
	log.Printf("✔️ Redis Ping: %v\n", pong)

	if err != nil {
		log.Fatalf("Redis Connection Failed: %v\n", c.rdb)
	}
}

//counterfeiter:generate . TaskStorage
type TaskStorage interface {
	SetNewTask(ctx context.Context, task *_pb.Task)
	FetchAndRemoveDueTasks(ctx context.Context) []*_pb.Task
	GetAllTasks(ctx context.Context) []*_pb.Task
	CountAllWaitingTasks(ctx context.Context) (int64, error)
	GetAllTasksPagination(ctx context.Context, offset, limit int32) []*_pb.Task
}

func (c *taskRedisClient) SetNewTask(ctx context.Context, task *_pb.Task) {
	task.TaskUuid = uuid.NewString()
	marshal, err := proto.Marshal(task)
	if err != nil {
		fmt.Println("❌  Error: marshal", err)
		return
	}

	c.rdb.ZAdd(ctx, c.config.Storage.SchedulerKeyName, redis.Z{
		Score:  getExecutionTimestamp(task),
		Member: marshal,
	})
}

func getExecutionTimestamp(task *_pb.Task) float64 {
	if task.Header == nil || len(task.Header) == 0 {
		return 0
	}

	//ExecutionTimestamp
	for k, v := range task.Header {
		if k == config.ExecutionTimestamp {
			floatVal, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return 0
			}
			return floatVal
		}
	}
	return 0
}

const ListOfAllTaskScriptThenRemove = `
local tasks = redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[2])
if ARGV[3] == "delete" then
	for i, task in ipairs(tasks) do
		redis.call("ZREM", KEYS[1], task)
	end
end
return tasks
`

const FetchAndRemoveScript = `
local currentTime = tonumber(ARGV[1])
local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', currentTime)
if #tasks > 0 then
	redis.call('ZREM', KEYS[1], unpack(tasks))
end
return tasks
`

// FetchAndRemoveDueTasks returns due tasks using FetchAndRemoveScript
func (c *taskRedisClient) FetchAndRemoveDueTasks(ctx context.Context) []*_pb.Task {
	unixMilli := time.Now().UnixMilli()

	cmd := c.rdb.Eval(ctx, FetchAndRemoveScript, []string{c.config.Storage.SchedulerKeyName}, float64(unixMilli))

	if cmd == nil {
		return nil
	}

	if cmd.Err() != nil {
		return nil
	}

	result, err := cmd.Result()
	if err != nil {
		log.Panicf("❌  Serve err: %v", err)
	}

	if result == nil {
		return nil
	}

	tasks := c.convertByteToTasks(result)

	return tasks
}

const ListOfAllTaskScript = `
	local tasks = redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[2])
    return tasks
    `

// GetAllTasks gat all tasks using ListOfAllTaskScript
func (c *taskRedisClient) GetAllTasks(ctx context.Context) []*_pb.Task {
	return nil
}

const GetAllTasksPagination = `
local key = KEYS[1]
local start = tonumber(ARGV[1])
local stop = tonumber(ARGV[2])
local elements = redis.call('ZRANGE', key, start, stop, 'WITHSCORES')
return elements
`

// GetAllTasksPagination gat all tasks using ListOfAllTaskScript
func (c *taskRedisClient) GetAllTasksPagination(ctx context.Context, offset, limit int32) []*_pb.Task {
	start := (offset - 1) * limit
	stop := start + limit - 1

	cmd := c.rdb.Eval(ctx, GetAllTasksPagination, []string{c.config.Storage.SchedulerKeyName}, start, stop)

	if cmd == nil {
		return nil
	}

	if cmd.Err() != nil {
		return nil
	}

	result, err := cmd.Result()
	if err != nil {
		log.Panicf("❌  Serve err: %v", err)
	}

	if result == nil {
		return nil
	}

	tasks := c.convertByteToTasks(result)

	return tasks
}

func (c *taskRedisClient) CountAllWaitingTasks(ctx context.Context) (int64, error) {
	return c.rdb.ZCard(ctx, c.config.Storage.SchedulerKeyName).Result()
}

type ConvertByteToTasksFn func(luaResult interface{}) []*_pb.Task

func ConvertByteToTasks(luaResult interface{}) []*_pb.Task {
	// Deserialize the protobuf messages from the result
	var ts []*_pb.Task

	// check for the correct type
	switch results := luaResult.(type) {
	case []interface{}:
		for _, item := range results {

			// Downcast to the original struct type
			str, ok := item.(string)
			if !ok {
				fmt.Println("❌  Downcast failed")
				continue
			}

			bytes := []byte(str)

			if len(bytes) < 32 {
				//fmt.Printf("📝 Bad Decoding: %x\n", bytes) // Print the byte data in hexadecimal format for debugging
				continue
			}

			var task _pb.Task

			// Convert item to []byte
			if err := proto.Unmarshal(bytes, &task); err != nil {
				fmt.Printf("❌  Failed to unmarshal protobuf: %v\n", err)
				continue
			}
			ts = append(ts, &task)
		}
	default:
		log.Printf("❌  Unexpected type for Lua script result: %T", luaResult)
	}
	return ts
}
