package core

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ehsaniara/delay-box/config"
	"github.com/ehsaniara/delay-box/kafka/kafkafakes"
	_pb "github.com/ehsaniara/delay-box/proto"
	"github.com/ehsaniara/delay-box/storage/storagefakes"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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
	//newScheduler := NewScheduler(ctx, fakeStorage, fakeSyncProducer, &c)
	s := &scheduler{
		storage:                      fakeStorage,
		producer:                     fakeSyncProducer,
		ctx:                          ctx,
		config:                       &c,
		convertParameterToTaskHeader: convertParameterToTaskHeader,
	}
	s.KafkaDispatcher(&sarama.ConsumerMessage{
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
	executionTime := time.Now().Add(2 * time.Second).UnixMilli()

	// payload
	key := "some Key"
	payloadMarshal := []byte("some payload data")

	// create kafka message from payload
	//newScheduler := NewScheduler(ctx, fakeStorage, fakeSyncProducer, &c)
	s := &scheduler{
		storage:                      fakeStorage,
		producer:                     fakeSyncProducer,
		ctx:                          ctx,
		config:                       &c,
		convertParameterToTaskHeader: convertParameterToTaskHeader,
	}
	s.KafkaDispatcher(&sarama.ConsumerMessage{
		Key: []byte(key),
		Headers: []*sarama.RecordHeader{{
			Key:   []byte(config.ExecutionTimestamp),
			Value: []byte(fmt.Sprintf("%d", executionTime)),
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	c := config.Config{
		Frequency: int32(10),
		Kafka: config.KafkaConfig{
			Enabled: true,
		},
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
		Headers: nil, // no header sent to KafkaDispatcher
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

	marshal, err := proto.Marshal(task)
	assert.NoError(t, err)

	assert.Equal(t, sarama.ByteEncoder(marshal), _message.Value)
	assert.Equal(t, 1, len(task.Header))
	//assert.Equal(t, len(task.Header), len(_message.Headers))
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
	s := &scheduler{
		storage:                      fakeStorage,
		producer:                     fakeSyncProducer,
		ctx:                          ctx,
		config:                       &c,
		convertParameterToTaskHeader: convertParameterToTaskHeader,
	}
	err := s.PublishNewTaskToKafka(task)
	assert.NoError(t, err)
	assert.Equal(t, 1, fakeSyncProducer.SendMessageCallCount())
}

func Test_convertParameterToHeader(t *testing.T) {
	tests := []struct {
		name string
		args map[string]string
		want map[string][]byte
	}{
		{name: "positive test", args: map[string]string{"test": "123"}, want: map[string][]byte{"test": []byte("123")}},
		{name: "empty arg", args: make(map[string]string), want: make(map[string][]byte)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertParameterToTaskHeader(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertParameterToHeader() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_scheduler_Schedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fakeStorage := &storagefakes.FakeTaskStorage{}
	fakeSyncProducer := &kafkafakes.FakeSyncProducer{}
	c := &config.Config{
		Kafka: config.KafkaConfig{
			Enabled: true,
		},
		Frequency: 100,
	}

	header := map[string]string{config.ExecutionTimestamp: fmt.Sprintf("%d", time.Now().Add(time.Second).UnixMilli())}
	pyloadStr := "VGVzdCBKYXkK"
	//taskType := "PUB_SUB"

	newScheduler := &scheduler{
		quit:     make(chan struct{}),
		storage:  fakeStorage,
		producer: fakeSyncProducer,
		ctx:      ctx,
		config:   c,
		convertParameterToTaskHeader: func(_header map[string]string) map[string][]byte {
			assert.Equal(t, header, _header)
			return convertParameterToTaskHeader(_header)
		},
	}

	err := newScheduler.Schedule(pyloadStr, header)
	assert.NoError(t, err)
	assert.Equal(t, 1, fakeSyncProducer.SendMessageCallCount())

	_call := fakeSyncProducer.SendMessageArgsForCall(0)
	decodeByte, err := base64.StdEncoding.DecodeString(pyloadStr)
	assert.NoError(t, err)

	assert.Equal(t, sarama.ByteEncoder(decodeByte), _call.Value)
}

func Test_scheduler_PublishNewTaskToRedis(t *testing.T) {
	ctx := context.Background()

	c := &config.Config{
		Kafka:   config.KafkaConfig{Enabled: false},
		Storage: config.StorageConfig{SchedulerChanel: "TestSchedulerChanel"},
	}

	task := &_pb.Task{
		Header: map[string][]byte{
			config.ExecutionTimestamp: []byte(strconv.FormatInt(time.Now().Add(2*time.Second).UnixMilli(), 10)),
		},
		Pyload: []byte("some payload data"),
	}

	marshal, _ := proto.Marshal(task)

	tests := []struct {
		name                 string
		task                 *_pb.Task
		marshal              []byte
		publishReturnsExpect *redis.IntCmd
		err                  error
	}{
		{
			name:                 "Happy Path",
			task:                 task,
			marshal:              marshal,
			publishReturnsExpect: redis.NewIntResult(0, nil),
			err:                  nil,
		},
		{
			name:    "Marshall Error",
			task:    nil,
			marshal: nil,
			err:     errors.New("marshall error"),
		},
		{
			name:                 "Publisher Error",
			task:                 task,
			marshal:              marshal,
			publishReturnsExpect: redis.NewIntResult(0, errors.New("publisher error")),
			err:                  errors.New("publisher error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			fakeTaskStorage := &storagefakes.FakeTaskStorage{}
			s := &scheduler{
				storage: fakeTaskStorage,
				ctx:     ctx,
				config:  c,
			}

			if tt.marshal != nil {

				fakeTaskStorage.PublishReturns(tt.publishReturnsExpect)

				res := s.PublishNewTaskToRedis(tt.task)
				assert.Equal(t, tt.err, res)

				assert.Equal(t, 1, fakeTaskStorage.PublishCallCount())

				_ctx, _chanel, _marshal := fakeTaskStorage.PublishArgsForCall(0)
				assert.Equal(t, ctx, _ctx)
				assert.Equal(t, "TestSchedulerChanel", _chanel)
				assert.Equal(t, marshal, _marshal)
			}

		})
	}
}

func Test_scheduler_PublishNewTaskToKafka(t *testing.T) {
	ctx := context.Background()

	c := &config.Config{
		Kafka: config.KafkaConfig{Enabled: true},
	}

	task := &_pb.Task{
		Header: map[string][]byte{
			config.ExecutionTimestamp: []byte(strconv.FormatInt(time.Now().Add(2*time.Second).UnixMilli(), 10)),
		},
		Pyload: []byte("some payload data"),
	}

	tests := []struct {
		name string
		task *_pb.Task
		err  error
	}{
		{
			name: "Happy Path",
			task: task,
			err:  nil,
		},
		{
			name: "SendMessage With Error",
			task: task,
			err:  errors.New("marshall error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			fakeSyncProducer := &kafkafakes.FakeSyncProducer{}
			s := &scheduler{
				producer: fakeSyncProducer,
				ctx:      ctx,
				config:   c,
			}

			fakeSyncProducer.SendMessageReturns(0, 0, tt.err)

			res := s.PublishNewTaskToKafka(tt.task)
			assert.Equal(t, 1, fakeSyncProducer.SendMessageCallCount())
			assert.Equal(t, tt.err, res)
		})
	}
}
