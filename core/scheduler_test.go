package core

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ehsaniara/scheduler/config"
	"github.com/ehsaniara/scheduler/kafka/kafkafakes"
	_pb "github.com/ehsaniara/scheduler/proto"
	"github.com/ehsaniara/scheduler/storage/storagefakes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_Dispatcher(t *testing.T) {
	schedulerKeyName := "SchedulerKeyName"
	taskExecutionTopic := "TET"
	schedulerTopic := "STT"

	fakeStorage := &storagefakes.FakeTaskStorage{}
	fakeSyncProducer := &kafkafakes.FakeSyncProducer{}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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

	// 2 seconds from now
	executionTime := float64(time.Now().Add(2 * time.Second).UnixMilli())

	// payload
	key := "some Key"
	payloadMarshal := []byte("some payload data")

	// create kafka message from payload
	newScheduler := NewScheduler(ctx, fakeStorage, fakeSyncProducer, &c)
	newScheduler.Dispatcher(&sarama.ConsumerMessage{
		Key: []byte(key),
		Headers: []*sarama.RecordHeader{{
			Key:   []byte("executionTimestamp"),
			Value: []byte(fmt.Sprintf("%v", executionTime)),
		}},
		Value: payloadMarshal,
		Topic: schedulerTopic,
	})

	task := &_pb.Task{
		ExecutionTimestamp: executionTime,
		Header:             make(map[string][]byte),
		Pyload:             payloadMarshal,
	}

	// create task from kafka message
	//taskMarshal, err := proto.Marshal(task)
	//assert.NoError(t, err)
	//assert.NotNil(t, taskMarshal)

	_ctx, _task := fakeStorage.SetNewTaskArgsForCall(0)
	assert.Equal(t, ctx, _ctx)
	assert.Equal(t, task.Header, _task.Header)
	assert.Equal(t, task.ExecutionTimestamp, _task.ExecutionTimestamp)
	assert.Equal(t, task.Pyload, _task.Pyload)

}

func Test_Serve(t *testing.T) {
	schedulerKeyName := "schedulerKeyName"

	fakeStorage := &storagefakes.FakeTaskStorage{}
	fakeSyncProducer := &kafkafakes.FakeSyncProducer{}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := config.Config{
		Storage: config.StorageConfig{
			SchedulerKeyName: schedulerKeyName,
		},
	}

	NewScheduler(ctx, fakeStorage, fakeSyncProducer, &c)

	assert.Equal(t, 1, fakeStorage.FetchAndRemoveDueTasksCallCount())

	_ctx := fakeStorage.FetchAndRemoveDueTasksArgsForCall(0)
	assert.Equal(t, ctx, _ctx)
}

func Test_scheduler_run_Eval_with_value(t *testing.T) {
	schedulerKeyName := "schedulerKeyName"
	taskExecutionTopic := "TaskExecution"

	fakeStorage := &storagefakes.FakeTaskStorage{}
	fakeSyncProducer := &kafkafakes.FakeSyncProducer{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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

	// create task
	var tasks []*_pb.Task
	task := &_pb.Task{
		ExecutionTimestamp: float64(executionTime),
		Header:             make(map[string][]byte),
		Pyload:             []byte("some task"),
	}

	tasks = append(tasks, task)

	message := &sarama.ProducerMessage{
		Topic: c.Kafka.TaskExecutionTopic,
		Value: sarama.ByteEncoder(task.Pyload),
	}

	fakeStorage.FetchAndRemoveDueTasksReturnsOnCall(0, tasks)

	// before call
	assert.Equal(t, 0, fakeStorage.FetchAndRemoveDueTasksCallCount())

	// call
	NewScheduler(ctx, fakeStorage, fakeSyncProducer, &c)

	// evaluate
	// in the task loop
	require.Equal(t, 1, fakeStorage.FetchAndRemoveDueTasksCallCount())
	//after task's timer executed
	time.Sleep(250 * time.Millisecond)
	require.Equal(t, 2, fakeStorage.FetchAndRemoveDueTasksCallCount())

	assert.Equal(t, 1, fakeSyncProducer.SendMessageCallCount())

	_message := fakeSyncProducer.SendMessageArgsForCall(0)
	assert.Equal(t, taskExecutionTopic, _message.Topic)
	assert.Equal(t, message.Topic, _message.Topic)
	encode, err := _message.Value.Encode()
	assert.NoError(t, err)
	assert.Equal(t, task.Pyload, encode)
}
