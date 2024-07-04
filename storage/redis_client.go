package storage

// You only need **one** of these per package!
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"context"
	"fmt"
	"github.com/ehsaniara/scheduler/config"
	"github.com/ehsaniara/scheduler/interfaces"
	_pb "github.com/ehsaniara/scheduler/proto"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"log"
	"time"
)

//counterfeiter:generate . TaskStorage
type TaskStorage interface {
	SetNewTask(ctx context.Context, task *_pb.Task)
	FetchAndRemoveDueTasks(ctx context.Context) []*_pb.Task
	GetAllTasks() []*_pb.Task
	CountAllWaitingTasks(ctx context.Context) (int64, error)
}

// indicator to see if *redis.Client are exist
var _ interfaces.RedisDBClient = (*redis.Client)(nil)

// NewRedisClient returns RedisDBClient to use in application, and close func
func NewRedisClient(ctx context.Context, config *config.Config) (TaskStorage, func()) {

	if len(config.Storage.RedisHost) == 0 {
		log.Fatal("redisHost is missing")
	}

	client := &taskRedisClient{
		config: config,
	}
	client.SetUp(ctx, config.Storage.RedisHost, config.Storage.RedisPass, config.Storage.RedisDb)

	return client, client.Close
}

type taskRedisClient struct {
	rdb    interfaces.RedisDBClient
	config *config.Config
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

func (c *taskRedisClient) Close() {
	log.Printf("⏳ Redis connection pool Closing...\n")
	err := c.rdb.Close()
	if err != nil {
		log.Printf("Error Close taskRedisClient:%v \n", err)
	}
	log.Printf("👍 Redis connection pool Closed!\n")
}

func (c *taskRedisClient) SetNewTask(ctx context.Context, task *_pb.Task) {
	marshal, err := proto.Marshal(task)
	if err != nil {
		fmt.Println("❌  Error: marshal", err)
		return
	}

	c.rdb.ZAdd(ctx, c.config.Storage.SchedulerKeyName, redis.Z{
		Score:  task.ExecutionTimestamp,
		Member: marshal,
	})
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

// language=lua
const ListOfAllTaskScript = `
    return redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[2])
    `

// language=lua
const FetchAndRemoveScript = `
local currentTime = tonumber(ARGV[1])
local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', currentTime)
if #tasks > 0 then
	redis.call('ZREM', KEYS[1], unpack(tasks))
end
return tasks
`

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

	tasks := ConvertByteToTasks(result)

	return tasks
}

func (c *taskRedisClient) GetAllTasks() []*_pb.Task {
	return nil
}

func (c *taskRedisClient) CountAllWaitingTasks(ctx context.Context) (int64, error) {

	return c.rdb.ZCard(ctx, c.config.Storage.SchedulerKeyName).Result()
}

func ConvertByteToTasks(luaResult interface{}) []*_pb.Task {
	// Deserialize the protobuf messages from the result
	var ts []*_pb.Task

	// check for the correct type
	switch results := luaResult.(type) {
	case []interface{}:
		for _, item := range results {
			var task _pb.Task
			// Convert item to []byte
			err := proto.Unmarshal([]byte(item.(string)), &task)
			//err := proto.Unmarshal(item.([]byte), &task)
			if err != nil {
				log.Fatalf("Failed to unmarshal protobuf: %v", err)
			}
			ts = append(ts, &task)
		}
	default:
		log.Fatalf("Unexpected type for Lua script result: %T", luaResult)
	}
	return ts
}
