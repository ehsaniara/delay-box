package storage

import (
	"context"
	"fmt"
	"github.com/ehsaniara/scheduler/interfaces/interfacesfakes"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/goleak"
	"log"
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

func TestNewRedisClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()

	redisContainer, err := setupContainers(ctx)
	defer redisContainer.Terminate(ctx)
	assert.NoError(t, err)

	redisHost, _ := redisContainer.Host(ctx)
	redisPort, err := redisContainer.MappedPort(ctx, "6379")
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())

	client, closeFunc := NewRedisClient(ctx, redisAddr, "", 0)
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
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())

	client := &redisClient{}
	client.SetUp(ctx, redisAddr, "", 0)

	require.NotNil(t, client.rdb, "Redis client should be initialized")
	assert.Equal(t, redisAddr, client.rdb.Options().Addr, "Redis host should match")
	assert.Equal(t, "", client.rdb.Options().Password, "Redis password should match")

	// Test ping to ensure connection is active
	pong, err := client.rdb.Ping(ctx).Result()
	require.NoError(t, err, "Ping should not return an error")
	assert.Equal(t, "PONG", pong, "Ping response should be PONG")
}

// TestEval checks the Eval method with a mock Redis client.
func TestEval(t *testing.T) {
	ctx := context.Background()

	// Mock Redis client
	fakeRedis := &interfacesfakes.FakeRedisDBClient{}
	fakeCmd := &redis.Cmd{}
	fakeCmd.SetVal([]interface{}{"result1", "result2"})
	fakeRedis.EvalReturns(fakeCmd)

	client := &redisClient{rdb: fakeRedis}

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

	client := &redisClient{rdb: fakeRedis}

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
