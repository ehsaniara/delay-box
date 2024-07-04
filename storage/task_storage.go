package storage

import (
	"context"
	"fmt"
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
	GetAllTasks(ctx context.Context, offset, limit int32) []*_pb.Task
	CountAllWaitingTasks(ctx context.Context) (int64, error)
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

	tasks := ConvertByteToTasks(result)

	return tasks
}

const ListOfAllTaskScript = `
	local tasks = redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[2])
    return tasks
    `

// GetAllTasks gat all tasks using ListOfAllTaskScript
func (c *taskRedisClient) GetAllTasks(ctx context.Context, offset, limit int32) []*_pb.Task {
	cmd := c.rdb.Eval(ctx, ListOfAllTaskScript, []string{c.config.Storage.SchedulerKeyName}, offset, limit)

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

func (c *taskRedisClient) CountAllWaitingTasks(ctx context.Context) (int64, error) {

	card := c.rdb.ZCard(ctx, c.config.Storage.SchedulerKeyName)
	if card.Err() != nil {
		return 0, card.Err()
	}

	return card.Result()
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
