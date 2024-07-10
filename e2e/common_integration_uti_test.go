package e2e

import (
	"bufio"
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ehsaniara/scheduler/config"
	"github.com/ehsaniara/scheduler/kafka"
	_task "github.com/ehsaniara/scheduler/proto"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	_kafkaContainer "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/proto"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
)

func consumerForTaskExecutionTopic(t *testing.T, ctx context.Context, wg *sync.WaitGroup, c *config.Config, expectedTask *_task.Task) {

	consumerGroup := kafka.NewConsumerGroup(c)
	kafka.NewConsumer(
		strings.Split(c.Kafka.TaskExecutionTopic, `,`),
		consumerGroup,
		func(message *sarama.ConsumerMessage) {
			assert.NotNil(t, message.Value)

			var task _task.Task
			err := proto.Unmarshal(message.Value, &task)
			assert.NoError(t, err)

			assert.Equal(t, expectedTask.TaskType, task.TaskType)
			assert.Equal(t, expectedTask.TaskUuid, task.TaskUuid)

			log.Printf("TaskExecutionTopic consumerGroup task: %v", task)
			wg.Done()
		},
	).Start(ctx)
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

func setupContainers(ctx context.Context) (testcontainers.Container, *_kafkaContainer.KafkaContainer, error) {

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
	kafkaContainer, err := _kafkaContainer.RunContainer(ctx, _kafkaContainer.WithClusterID(clusterID), testcontainers.WithImage("confluentinc/confluent-local:7.5.0"))
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
