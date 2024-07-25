package storage

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ehsaniara/delay-box/config"
	"github.com/ehsaniara/delay-box/interfaces/interfacesfakes"
	_pb "github.com/ehsaniara/delay-box/proto"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/proto"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

func setupContainers(ctx context.Context) (testcontainers.Container, error) {

	// Start a Redis container
	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:latest",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp"),
		},
		Started: true,
	}
	redisContainer, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		log.Fatalf("failed to start container: %s", err)
	}

	return redisContainer, nil
}
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

	client := &taskRedisClient{
		config:             &c,
		rdb:                fakeRedisDBClient,
		convertByteToTasks: ConvertByteToTasks,
	}

	executionTime := time.Now().Add(100 * time.Millisecond).UnixMilli() // Schedule 0.1 seconds from now

	// create task
	task := &_pb.Task{
		Header: map[string][]byte{config.ExecutionTimestamp: []byte(strconv.FormatInt(executionTime, 10))},
		Pyload: []byte("some task"),
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

	client := &taskRedisClient{
		config:             &c,
		rdb:                fakeRedisDBClient,
		convertByteToTasks: ConvertByteToTasks,
	}

	fakeRedisDBClient.ZCardReturnsOnCall(0, &redis.IntCmd{})

	// call the method
	_, _ = client.CountAllWaitingTasks(ctx)

	assert.Equal(t, 1, fakeRedisDBClient.ZCardCallCount())

}

func Test_taskRedisClient_FetchAndRemoveDueTasks(t *testing.T) {
	schedulerKeyName := "schedulerKeyName"
	ctx := context.Background()
	c := config.Config{
		Storage: config.StorageConfig{
			RedisHost:        "testHost",
			SchedulerKeyName: schedulerKeyName,
		},
	}

	fakeRedisDBClient := &interfacesfakes.FakeRedisDBClient{}

	client := &taskRedisClient{
		config:             &c,
		rdb:                fakeRedisDBClient,
		convertByteToTasks: ConvertByteToTasks,
	}

	client.FetchAndRemoveDueTasks(ctx)

	assert.Equal(t, 1, fakeRedisDBClient.EvalCallCount())

	_ctx, _script, _skn, _ := fakeRedisDBClient.EvalArgsForCall(0)
	assert.Equal(t, ctx, _ctx)
	assert.Equal(t, FetchAndRemoveScript, _script)
	assert.Equal(t, []string{schedulerKeyName}, _skn)
}

func Test_taskRedisClient_GetAllTasksPagination(t *testing.T) {
	schedulerKeyName := "schedulerKeyName"
	executionTime := time.Now().Add(100 * time.Millisecond).UnixMilli() // Schedule 0.1 seconds from now
	ctx := context.Background()

	offset := int64(0)
	limit := int64(100)

	c := config.Config{
		Storage: config.StorageConfig{
			RedisHost:        "testHost",
			SchedulerKeyName: schedulerKeyName,
		},
	}

	start := (offset - 1) * limit
	stop := start + limit - 1

	fakeRedisDBClient := &interfacesfakes.FakeRedisDBClient{}

	var ts []*_pb.Task
	ts = append(ts, &_pb.Task{
		Header: map[string][]byte{config.ExecutionTimestamp: []byte(strconv.FormatInt(executionTime, 10))},
		Pyload: []byte("some task"),
	})

	tests := []struct {
		name  string
		cmd   *redis.Cmd
		tasks []*_pb.Task
	}{
		{
			name:  "EvalReturns nil",
			cmd:   nil,
			tasks: nil,
		},
		{
			name:  "EvalReturns none nil cmd",
			cmd:   redis.NewCmd(ctx, ts),
			tasks: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeRedisDBClient.EvalReturns(tt.cmd)

			client := &taskRedisClient{
				config: &c,
				rdb:    fakeRedisDBClient,
				convertByteToTasks: func(luaResult interface{}) []*_pb.Task {
					return ts
				},
			}
			paginationList := client.GetAllTasksPagination(ctx, int32(offset), int32(limit))

			_ctx, _script, _keys, _arg := fakeRedisDBClient.EvalArgsForCall(0)
			assert.Equal(t, ctx, _ctx)
			assert.Equal(t, GetAllTasksPagination, _script)
			assert.Equal(t, []string{c.Storage.SchedulerKeyName}, _keys)
			assert.Equal(t, int32(start), _arg[0])
			assert.Equal(t, int32(stop), _arg[1])

			assert.Equal(t, len(tt.tasks), len(paginationList))
		})
	}
}

func TestNewRedisClient(t *testing.T) {
	schedulerKeyName := "schedulerKeyName"
	defer goleak.VerifyNone(t)

	ctx := context.Background()

	redisContainer, err := setupContainers(ctx)
	assert.NoError(t, err)
	defer redisContainer.Terminate(ctx)

	redisHost, err := redisContainer.Host(ctx)
	assert.NoError(t, err)
	redisPort, err := redisContainer.MappedPort(ctx, "6379")
	assert.NoError(t, err)

	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())

	c := config.Config{
		Storage: config.StorageConfig{
			RedisHost:        redisAddr,
			SchedulerKeyName: schedulerKeyName,
		},
	}

	client, closeFunc := NewRedisClient(ctx, &c)
	defer closeFunc()

	assert.NotNil(t, client, "Redis client should not be nil")
}

func TestRedisClient_SetUp(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()

	redisContainer, err := setupContainers(ctx)
	defer redisContainer.Terminate(ctx)
	assert.NoError(t, err)

	redisHost, _ := redisContainer.Host(ctx)
	redisPort, err := redisContainer.MappedPort(ctx, "6379")
	assert.NoError(t, err)
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())

	client := &taskRedisClient{}
	client.SetUp(ctx, redisAddr, "", 0)

	require.NotNil(t, client.rdb, "Redis client should be initialized")
	assert.Equal(t, redisAddr, client.rdb.Options().Addr, "Redis host should match")
	assert.Equal(t, "", client.rdb.Options().Password, "Redis password should match")

	// Test ping to ensure connection is active
	pong, err := client.rdb.Ping(ctx).Result()
	require.NoError(t, err, "Ping should not return an error")
	assert.Equal(t, "PONG", pong, "Ping response should be PONG")
}

// A helper function to replace log.Fatal in tests
func replaceLogFatal(f func(format string, v ...interface{})) func() {
	old := logFatal
	logFatal = f
	return func() { logFatal = old }
}

func TestRedisClient_SetUp_fail(t *testing.T) {
	// Capture the log output
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr) // Restore original log output

	// Replace logFatal with a custom function
	restoreLogFatal := replaceLogFatal(func(format string, v ...interface{}) {
		fmt.Fprintf(&buf, format, v...)
	})

	// Ensure logFatal is restored after the test
	defer restoreLogFatal()

	defer goleak.VerifyNone(t)

	ctx := context.Background()

	client := &taskRedisClient{}
	client.SetUp(ctx, "", "", 0)
	//t.Error("redisHost is missing", buf.String())
}

// TestEval checks the Eval method with a mock Redis client.
func TestEval(t *testing.T) {
	ctx := context.Background()

	// Mock Redis client
	fakeRedis := &interfacesfakes.FakeRedisDBClient{}
	fakeCmd := &redis.Cmd{}
	fakeCmd.SetVal([]interface{}{"result1", "result2"})
	fakeRedis.EvalReturns(fakeCmd)

	client := &taskRedisClient{rdb: fakeRedis}

	script := "return redis.call('keys', '*')"
	keys := []string{}
	args := []interface{}{}

	result := client.rdb.Eval(ctx, script, keys, args...)

	// Verify the result
	res, err := result.Result()
	require.NoError(t, err, "Eval should not return an error")
	assert.Equal(t, []interface{}{"result1", "result2"}, res, "Eval should return the correct result")
}

// TestEval checks the Eval method with a mock Redis client.
func TestZAdd(t *testing.T) {
	ctx := context.Background()

	// Mock Redis client
	fakeRedis := &interfacesfakes.FakeRedisDBClient{}
	intCmd := redis.NewIntResult(1, nil)
	fakeRedis.ZAddReturns(intCmd)

	client := &taskRedisClient{rdb: fakeRedis}

	now := time.Now()

	script := "return redis.call('keys', '*')"
	f := float64(now.Add(10 * time.Second).UnixMilli())

	result := client.rdb.ZAdd(ctx, script, redis.Z{
		Score:  f,
		Member: []byte("some data"),
	})

	// Verify the result
	res, err := result.Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), res)
}

// TestConvertByteToTasks, table test for ConvertByteToTasks
func TestConvertByteToTasks(t *testing.T) {

	tests := []struct {
		name     string
		args     interface{}
		expected []*_pb.Task
	}{
		{
			name:     "invalid test",
			args:     nil,
			expected: nil,
		},
		{
			name:     "empty test",
			args:     struct{ luaResult []interface{} }{},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.expected, ConvertByteToTasks(tt.args), "ConvertByteToTasks(%v)", tt.args)
		})
	}
}

// TestConvertByteToTasks_ValidProtobufMessages proto.Marshal(task) serializes the task struct into a byte slice (data).
// This byte slice contains the protobuf-encoded representation of your _pb.Task message.
// The main function demonstrates how to create an instance of _pb.Task and serialize it.
// In a real application or test scenario, you would use proto.Marshal to serialize the _pb.Task instances you want to test with.
func TestConvertByteToTasks_ValidProtobufMessages(t *testing.T) {
	executionTime := time.Now().Add(100 * time.Millisecond).UnixMilli() // Schedule 0.1 seconds from now
	task := _pb.Task{
		Header: map[string][]byte{config.ExecutionTimestamp: []byte(strconv.FormatInt(executionTime, 10))},
		Pyload: []byte("----- some task ------"), // length should be grater than 32bit
		Status: _pb.Task_PENDING,
	}

	// Serialize the task into bytes
	data, err := proto.Marshal(&task)
	assert.NoError(t, err)

	// Print the serialized data as a string (for testing purposes)
	fmt.Println(string(data))

	// Mock Redis response with serialized protobuf messages (strings)
	mockLuaResult := []interface{}{
		string(data), // task1
		string(data), // task2
	}

	expectedTasks := []*_pb.Task{
		&task, // task1
		&task, // task2
	}

	// Call the function
	tasks := ConvertByteToTasks(mockLuaResult)

	for i, actualTask := range tasks {
		assert.Equal(t, expectedTasks[i].TaskUuid, actualTask.TaskUuid)
		assert.Equal(t, expectedTasks[i].Header, actualTask.Header)
	}

	// Assert the result
	assert.Len(t, tasks, 2) // Ensure two tasks were converted
	// Add more specific assertions based on your protobuf message structure and content
}

func TestConvertByteToTasks_InvalidType(t *testing.T) {
	// Mock Redis response with an invalid type (not a []interface{})
	mockLuaResult := "not_an_array"

	// Call the function
	tasks := ConvertByteToTasks(mockLuaResult)

	// Assert the result
	assert.Empty(t, tasks) // Ensure an empty slice is returned
	// Optionally, assert the log message output if using capture logs
}

func TestConvertByteToTasks_ErrorUnmarshaling(t *testing.T) {
	// Mock Redis response with a malformed serialized protobuf message
	mockLuaResult := []interface{}{
		"malformed_serialized_message",
	}

	// Call the function
	tasks := ConvertByteToTasks(mockLuaResult)

	// Assert the result
	assert.Len(t, tasks, 0) // Ensure no tasks were converted due to error
	// Optionally, assert the log message output if using capture logs
}
