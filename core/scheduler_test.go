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
	"reflect"
	"strconv"
	"testing"
	"time"
)

func Test_Dispatcher_with_no_header(t *testing.T) {
	fakeStorage := &storagefakes.FakeTaskStorage{}
	fakeSyncProducer := &kafkafakes.FakeSyncProducer{}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := config.Config{
		Storage: config.StorageConfig{
			SchedulerKeyName: "SchedulerKeyName",
		},
		Kafka: config.KafkaConfig{
			Enabled:            true,
			SchedulerTopic:     "STT",
			TaskExecutionTopic: "TET",
		},
	}

	// payload
	// create kafka message from payload
	newScheduler := NewScheduler(ctx, fakeStorage, fakeSyncProducer, &c)
	newScheduler.Dispatcher(&sarama.ConsumerMessage{
		Key:   []byte(("some Key")),
		Value: []byte("some payload data"),
		Topic: "STT",
	})

	assert.Equal(t, 0, fakeStorage.SetNewTaskCallCount())

}
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
			Key:   []byte(config.ExecutionTimestamp),
			Value: []byte(fmt.Sprintf("%v", executionTime)),
		}},
		Value: payloadMarshal,
		Topic: schedulerTopic,
	})

	task := &_pb.Task{
		Header: map[string][]byte{config.ExecutionTimestamp: []byte(fmt.Sprintf("%v", executionTime))},
		Pyload: payloadMarshal,
	}

	_ctx, _task := fakeStorage.SetNewTaskArgsForCall(0)
	assert.Equal(t, ctx, _ctx)
	assert.Equal(t, task.Header, _task.Header)
	assert.Equal(t, task.Pyload, _task.Pyload)

}

func Test_Serve(t *testing.T) {
	schedulerKeyName := "schedulerKeyName"

	fakeStorage := &storagefakes.FakeTaskStorage{}
	fakeSyncProducer := &kafkafakes.FakeSyncProducer{}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := config.Config{
		Frequency: int32(10),
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
		Frequency: int32(50),
		Storage: config.StorageConfig{
			SchedulerKeyName: schedulerKeyName,
		},
		Kafka: config.KafkaConfig{
			Enabled:            true,
			TaskExecutionTopic: taskExecutionTopic,
		},
	}

	f1 := time.Duration(c.Frequency)
	executionTime := time.Now().Add(f1 * time.Millisecond).UnixMilli() // Schedule 0.1 seconds from now

	// create task
	var tasks []*_pb.Task
	task := &_pb.Task{
		Header: map[string][]byte{config.ExecutionTimestamp: []byte(strconv.FormatInt(executionTime, 10))},
		Pyload: []byte("some task"),
	}

	tasks = append(tasks, task)

	message := &sarama.ProducerMessage{
		Topic:   c.Kafka.TaskExecutionTopic,
		Value:   sarama.ByteEncoder(task.Pyload),
		Headers: nil, // no header sent to Dispatcher
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
	f2 := time.Duration(c.Frequency + 5)
	time.Sleep(f2 * time.Millisecond)
	require.Equal(t, 2, fakeStorage.FetchAndRemoveDueTasksCallCount())

	assert.Equal(t, 1, fakeSyncProducer.SendMessageCallCount())

	_message := fakeSyncProducer.SendMessageArgsForCall(0)
	assert.Equal(t, taskExecutionTopic, _message.Topic)
	assert.Equal(t, message.Topic, _message.Topic)
	encode, err := _message.Value.Encode()
	assert.NoError(t, err)
	assert.Equal(t, task.Pyload, encode)
	assert.Equal(t, len(task.Header), 1)
	assert.Equal(t, len(task.Header), len(_message.Headers))
	//for k, v := range task.Header {
	//	assert.Equal(t, string(task[k]), encode)
	//}
}

func Test_scheduler_PublishNewTask(t *testing.T) {
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
	executionTime := time.Now().Add(2 * time.Second).UnixMilli()

	payloadMarshal := []byte("some payload data")

	task := &_pb.Task{
		Header: map[string][]byte{config.ExecutionTimestamp: []byte(strconv.FormatInt(executionTime, 10))},
		Pyload: payloadMarshal,
	}
	// create kafka message from payload
	newScheduler := NewScheduler(ctx, fakeStorage, fakeSyncProducer, &c)
	newScheduler.PublishNewTask(task)
	assert.Equal(t, 1, fakeSyncProducer.SendMessageCallCount())
}

func Test_convertParameterToHeader(t *testing.T) {
	tests := []struct {
		name string
		args map[string]string
		want map[string][]byte
	}{
		{name: "positive test", args: map[string]string{"test": "dGVzdA=="}, want: map[string][]byte{"test": []byte("test")}},
		{name: "empty arg", args: make(map[string]string), want: make(map[string][]byte)},
		{name: "wrong base64 arg", args: map[string]string{"+++": "+++"}, want: make(map[string][]byte)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertParameterToHeader(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertParameterToHeader() = %v, want %v", got, tt.want)
			}
		})
	}
}
