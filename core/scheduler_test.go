package core

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ehsaniara/scheduler/config"
	"github.com/ehsaniara/scheduler/interfaces/interfacesfakes"
	"github.com/ehsaniara/scheduler/kafka/kafkafakes"
	_pb "github.com/ehsaniara/scheduler/proto"
	"github.com/golang/protobuf/proto"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_scheduler_Dispatcher(t *testing.T) {
	schedulerKeyName := "SchedulerKeyName"
	taskExecutionTopic := "TaskExecution"
	schedulerTopic := "SchedulerTopic"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fakeRedisDBClient := &interfacesfakes.FakeRedisDBClient{}
	fakeSyncProducer := &kafkafakes.FakeSyncProducer{}
	c := config.Config{
		Storage: config.StorageConfig{
			SchedulerKeyName: schedulerKeyName,
		},
		Kafka: config.KafkaConfig{
			Enabled:            true,
			SchedulerTopic:     schedulerTopic,
			TaskExecutionTopic: taskExecutionTopic,
		},
	}

	now := time.Now()
	executionTime := float64(now.Add(2 * time.Second).UnixMilli()) // Schedule 2 seconds from now

	key := "gameUuid"
	marshal := []byte("some payload data")

	newScheduler := NewScheduler(ctx, fakeRedisDBClient, fakeSyncProducer, &c)
	newScheduler.Dispatcher(&sarama.ConsumerMessage{
		Key: []byte(key),
		Headers: []*sarama.RecordHeader{{
			Key:   []byte("executionTimestamp"),
			Value: []byte(fmt.Sprintf("%v", executionTime)),
		}},
		Value: marshal, Topic: schedulerTopic,
	})

	task := _pb.Task{
		ExecutionTimestamp: executionTime,
		Header:             make(map[string][]byte),
		Pyload:             marshal,
	}

	marshal, err := proto.Marshal(&task)
	assert.NoError(t, err)

	_ctx, _name, _payload := fakeRedisDBClient.ZAddArgsForCall(0)
	assert.Equal(t, ctx, _ctx)
	assert.Equal(t, schedulerKeyName, _name)
	assert.Equal(t, []redis.Z{
		{
			Score:  executionTime,
			Member: marshal,
		},
	}, _payload)

}

func Test_scheduler_run(t *testing.T) {
	schedulerKeyName := "schedulerKeyName"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fakeRedisDBClient := &interfacesfakes.FakeRedisDBClient{}
	fakeSyncProducer := &kafkafakes.FakeSyncProducer{}

	c := config.Config{
		Storage: config.StorageConfig{
			Chanel:           "",
			SchedulerKeyName: schedulerKeyName,
		},
	}

	NewScheduler(ctx, fakeRedisDBClient, fakeSyncProducer, &c)

	now := float64(time.Now().UnixMilli())
	// this is to make sure goroutines is running
	//time.Sleep(20 * time.Millisecond)

	assert.Equal(t, 1, fakeRedisDBClient.EvalCallCount())

	_ctx, _script, _keys, _timestamp := fakeRedisDBClient.EvalArgsForCall(0)
	assert.Equal(t, ctx, _ctx)
	assert.Equal(t, FetchAndRemoveScript, _script)
	assert.Equal(t, []string{schedulerKeyName}, _keys)
	// with 10 millisecond Comparisons
	assert.InDelta(t, now, _timestamp[0], 10)
}

func Test_scheduler_run_Eval_with_value(t *testing.T) {
	schedulerKeyName := "schedulerKeyName"
	taskExecutionTopic := "TaskExecution"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fakeRedisDBClient := &interfacesfakes.FakeRedisDBClient{}
	fakeSyncProducer := &kafkafakes.FakeSyncProducer{}
	c := config.Config{
		Storage: config.StorageConfig{
			SchedulerKeyName: schedulerKeyName,
		},
		Kafka: config.KafkaConfig{
			Enabled:            true,
			TaskExecutionTopic: taskExecutionTopic,
		},
	}

	executionTime := time.Now().Add(100 * time.Millisecond).UnixMilli() // Schedule 0.1 seconds from now

	task := _pb.Task{
		ExecutionTimestamp: float64(executionTime),
		Header:             make(map[string][]byte),
		Pyload:             []byte("some task"),
	}
	marshal, err := proto.Marshal(&task)
	assert.NoError(t, err)

	tasks := []interface{}{marshal}

	eval := redis.NewCmd(ctx)
	eval.SetVal(tasks)

	result, err := eval.Result()
	assert.Nil(t, err)
	assert.NotNil(t, result)

	fakeRedisDBClient.EvalReturnsOnCall(0, eval)

	// call
	NewScheduler(ctx, fakeRedisDBClient, fakeSyncProducer, &c)

	// evaluate
	// in the task loop
	require.Equal(t, 1, fakeRedisDBClient.EvalCallCount())
	//after task's timer executed
	time.Sleep(250 * time.Millisecond)
	require.Equal(t, 2, fakeRedisDBClient.EvalCallCount())

	assert.Equal(t, 1, fakeSyncProducer.SendMessageCallCount())

	_message := fakeSyncProducer.SendMessageArgsForCall(0)
	assert.Equal(t, taskExecutionTopic, _message.Topic)
	assert.EqualValues(t, marshal, _message.Value)
}
