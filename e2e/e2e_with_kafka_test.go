package e2e

import (
	"bufio"
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ehsaniara/scheduler/config"
	_task "github.com/ehsaniara/scheduler/proto"
	"github.com/ehsaniara/scheduler/storage"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/proto"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func TestPositiveIntegrationTest(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	redisContainer, kafkaContainer, err := setupContainers(ctx)
	assert.NoError(t, err)

	brokers, err := kafkaContainer.Brokers(ctx)

	// Get the Redis container's address
	redisHost, err := redisContainer.Host(ctx)
	assert.NoError(t, err)
	redisPort, err := redisContainer.MappedPort(ctx, "6379")
	assert.NoError(t, err)
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())

	err = os.Setenv("REDIS_HOST", redisAddr)
	assert.NoError(t, err)
	err = os.Setenv("BROKERS", brokers[0])
	assert.NoError(t, err)
	defer func() {
		_ = os.Unsetenv("APP_CONF_PATH")
		_ = os.Unsetenv("REDIS_HOST")
		_ = os.Unsetenv("BROKERS")

		// Clean up the container
		if err = redisContainer.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate container: %s", err)
		}

		// Clean up the container after
		if err = kafkaContainer.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate container: %s", err)
		}
	}()

	// Start the server
	var wg sync.WaitGroup
	wg.Add(1)
	go runMainProcessInParallel(&wg)

	//
	err = os.Setenv("APP_CONF_PATH", "./kafka-test.yaml")
	assert.NoError(t, err)

	c := config.GetConfig()

	var redisClient, redisClientCloseFn = storage.NewRedisClient(ctx, c)
	defer redisClientCloseFn()

	// Wait for the server to start
	ready := make(chan error)
	go func() {
		for {
			time.Sleep(time.Second)
			// Perform a GET request
			resp, e := http.Get(fmt.Sprintf("http://localhost:%d/ping", c.HttpServer.Port))
			if e != nil {
				t.Logf("Failed to make GET request: %v", e)
			} else {
				if resp.StatusCode == http.StatusOK {
					err = resp.Body.Close()
					assert.NoError(t, err)
					ready <- nil
				}
			}
		}
	}()

	readyErr := <-ready
	assert.NoError(t, readyErr)

	//put new task in kafka
	sc := sarama.NewConfig()
	// set config to true because successfully delivered messages will be returned on the Successes channel
	sc.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, sc)
	if err != nil {
		t.Fatal(err)
	}

	// Schedule 1/4 seconds from now
	executionTime := float64(time.Now().Add(time.Millisecond * 250).UnixMilli())
	task := _task.Task{
		ExecutionTimestamp: executionTime,
		Header:             make(map[string][]byte),
		Pyload:             []byte("some payload data"),
	}
	m, e := proto.Marshal(&task)
	assert.NoError(t, e)

	if _, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: c.Kafka.SchedulerTopic,
		Value: sarama.ByteEncoder(m),
		Headers: []sarama.RecordHeader{{
			Key:   []byte("executionTimestamp"),
			Value: []byte(fmt.Sprintf("%v", executionTime)),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	//check if it's in redis, to make sure message get dispatched to redis from consumer
	time.Sleep(10 * time.Millisecond)

	count, err := redisClient.CountAllWaitingTasks(ctx)
	assert.NoError(t, err)

	// task should be in the redis, it's not expired yet
	assert.Equal(t, int64(1), count)

	//check that task got erased after that, task has only 1 sec TTL
	time.Sleep(1005 * time.Millisecond)

	count, err = redisClient.CountAllWaitingTasks(ctx)
	assert.NoError(t, err)

	assert.Equal(t, int64(0), count)

	wg.Done()
}

func runMainProcessInParallel(wg *sync.WaitGroup) {
	defer wg.Done()

	if err := os.Setenv("APP_CONF_PATH", "../e2e/kafka-test.yaml"); err != nil {
		log.Fatal(err)
	}

	cmd := exec.Command("go", "run", "../cmd/main.go")

	// Create pipes for the output
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Printf("Error creating StdoutPipe for Cmd: %v\n", err)
		return
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		fmt.Printf("Error creating StderrPipe for Cmd: %v\n", err)
		return
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		fmt.Printf("Error starting Cmd: %v\n", err)
		return
	}

	// Capture and print stdout
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			fmt.Printf(">>: %s\n", scanner.Text())
		}
	}()

	// Capture and print stderr
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			fmt.Printf("e>>: %s\n", scanner.Text())
		}
	}()

	// Wait for the command to finish
	if err = cmd.Wait(); err != nil {
		fmt.Printf("Cmd finished with error: %v\n", err)
	}

	// Ensure the server is stopped at the end of the test
	defer func() {
		if err := cmd.Process.Kill(); err != nil {
			log.Fatalf("Failed to stop server: %v", err)
		}
	}()
}

func setupContainers(ctx context.Context) (testcontainers.Container, *kafka.KafkaContainer, error) {

	clusterID := "test-cluster-id"

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

	redisState, err := redisContainer.State(ctx)
	if err != nil {
		log.Fatalf("failed to get container state: %s", err)
	}

	fmt.Println(redisState.Running)

	//setting-up kafka
	kafkaContainer, err := kafka.RunContainer(ctx, kafka.WithClusterID(clusterID), testcontainers.WithImage("confluentinc/confluent-local:7.5.0"))
	if err != nil {
		log.Fatalf("failed to start container: %s", err)
	}
	state, err := kafkaContainer.State(ctx)
	if err != nil {
		log.Fatalf("failed to get container state: %s", err)
	}
	if !state.Running {
		log.Fatalf("failed to get container state: %s", err)
	}
	if clusterID != kafkaContainer.ClusterID {
		log.Fatalf("it's not the same claster")
	}

	return redisContainer, kafkaContainer, nil
}
