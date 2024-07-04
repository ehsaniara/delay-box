package storage

import (
	"context"
	"github.com/ehsaniara/scheduler/config"
	"github.com/ehsaniara/scheduler/interfaces/interfacesfakes"
	_pb "github.com/ehsaniara/scheduler/proto"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

//func TestConvertByteToTask(t *testing.T) {
//	now := time.Now()
//	f := float64(now.Add(10 * time.Second).UnixMilli())
//	task := []*_pb.Task{
//		{
//			ExecutionTimestamp: f,
//			Header:             make(map[string][]byte),
//			Pyload:             []byte("Test Payload"),
//		},
//	}
//
//	marshal, err := proto.Marshal(task)
//	require.NoError(t, err)
//
//	var tasksInterface interface{} = marshal
//
//	toTask := ConvertByteToTasks(tasksInterface)
//
//	assert.Equal(t, &task, toTask)
//}

func Test_taskRedisClient_SetNewTask(t *testing.T) {
	schedulerKeyName := "schedulerKeyName"
	ctx := context.Background()
	c := config.Config{
		Storage: config.StorageConfig{
			RedisHost:        "testHost",
			SchedulerKeyName: schedulerKeyName,
		},
	}

	fakeRedisDBClient := &interfacesfakes.FakeRedisDBClient{}

	client, _ := NewRedisClient(ctx, &c, fakeRedisDBClient)

	executionTime := time.Now().Add(100 * time.Millisecond).UnixMilli() // Schedule 0.1 seconds from now

	// create task
	task := &_pb.Task{
		ExecutionTimestamp: float64(executionTime),
		Header:             make(map[string][]byte),
		Pyload:             []byte("some task"),
	}

	// call the method
	client.SetNewTask(ctx, task)

	assert.Equal(t, 1, fakeRedisDBClient.ZAddCallCount())

	_ctx, _schedulerKeyName, _ := fakeRedisDBClient.ZAddArgsForCall(0)
	assert.Equal(t, ctx, _ctx)
	assert.Equal(t, schedulerKeyName, _schedulerKeyName)
}

func Test_taskRedisClient_CountAllWaitingTasks(t *testing.T) {
	schedulerKeyName := "schedulerKeyName"
	ctx := context.Background()
	c := config.Config{
		Storage: config.StorageConfig{
			RedisHost:        "testHost",
			SchedulerKeyName: schedulerKeyName,
		},
	}

	fakeRedisDBClient := &interfacesfakes.FakeRedisDBClient{}

	client, _ := NewRedisClient(ctx, &c, fakeRedisDBClient)

	fakeRedisDBClient.ZCardReturnsOnCall(0, &redis.IntCmd{})

	// call the method
	_, _ = client.CountAllWaitingTasks(ctx)

	assert.Equal(t, 1, fakeRedisDBClient.ZCardCallCount())

}
