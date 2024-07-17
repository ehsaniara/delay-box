package worker

// You only need **one** of these per package!
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ehsaniara/delay-box/config"
	"github.com/ehsaniara/delay-box/kafka"
	_pb "github.com/ehsaniara/delay-box/proto"
	"github.com/ehsaniara/delay-box/storage"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"log"
	"os/exec"
	"strings"
)

//counterfeiter:generate . TaskExecutor
type TaskExecutor interface {
	ExecuteCommandConsumerKafka(message *sarama.ConsumerMessage)
	SetUpSubscriber(ctx context.Context)
	Stop()
}

type taskExecutor struct {
	quit      chan struct{}
	storage   storage.TaskStorage
	shellPath string
	consumer  kafka.Consumer
	config    *config.Config
	pubSub    *redis.PubSub
}

func NewTaskExecutor(config *config.Config, storage storage.TaskStorage) TaskExecutor {
	log.Printf("✔️ TaskExecutor is waiting to start..")
	// List of potential shells
	shells := []string{"sh", "bash", "zsh"}

	var shellPath string
	var err error

	// Find the first available shell
	for _, shell := range shells {
		shellPath, err = exec.LookPath(shell)
		if err == nil {
			break
		}
	}
	if shellPath == "" {
		log.Fatal("❌  No suitable shell found in PATH.")
	}

	log.Printf("🐚 Using shell: %s\n", shellPath)

	return &taskExecutor{
		quit:      make(chan struct{}),
		shellPath: shellPath,
		config:    config,
		storage:   storage,
	}
}

func (t *taskExecutor) SetUpSubscriber(ctx context.Context) {

	if t.config.Kafka.Enabled {
		cg := kafka.NewConsumerGroup(t.config)
		consumer := kafka.NewConsumer(
			strings.Split(t.config.Kafka.TaskExecutionTopic, `,`),
			cg,
			t.ExecuteCommandConsumerKafka,
		)
		consumer.Start(ctx)
		t.consumer = consumer
	} else {
		go t.Subscribe(ctx)
	}
	log.Printf("✔️ TaskExecutor is started.")

}

func (t *taskExecutor) ExecuteCommandConsumerKafka(message *sarama.ConsumerMessage) {

	var task _pb.Task

	if err := proto.Unmarshal(message.Value, &task); err != nil {
		fmt.Printf("❌  Failed to unmarshal protobuf: %v\n", err)
	}
	taskType := ""
	for k, v := range task.Header {
		if k == "taskType" {
			taskType = string(v)
		}
	}

	if taskType == "SHELL_CMD" {
		t.ExecuteSellCommand(&task)
	}
}

func (t *taskExecutor) ExecuteSellCommand(task *_pb.Task) {
	cmd := exec.Command(t.shellPath, "-c", string(task.Pyload))

	// Run the command and capture the output
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println("Error executing command:", err)
		return
	}

	// Print the output
	fmt.Println(string(output))
}

func (t *taskExecutor) Subscribe(ctx context.Context) {
	pubsub := t.storage.Subscribe(ctx, t.config.Storage.TaskExecutionChanel)

	// Wait for confirmation that subscription is created

	if _, err := pubsub.Receive(ctx); err != nil {
		fmt.Println("❌  Receive")
		return
	}

	t.pubSub = pubsub
	ch := pubsub.ChannelWithSubscriptions()

	for {
		select {
		case msgInterface := <-ch:
			switch msg := msgInterface.(type) {
			case *redis.Message:
				var task _pb.Task
				// we already know it should Unmarshal without error
				_ = proto.Unmarshal([]byte(msg.Payload), &task)

				taskType := ""
				for k, v := range task.Header {
					if k == "taskType" {
						taskType = string(v)
					}
				}

				if taskType == "SHELL_CMD" {
					t.ExecuteSellCommand(&task)
				}

			case *redis.Subscription:
				fmt.Printf("ExecuteCommand Subscription: kind=%s, channel=%s, count=%d\n", msg.Kind, msg.Channel, msg.Count)
			default:
				fmt.Println("Unknown message type")
			}

		case <-t.quit:
			log.Println("👍 ExecuteCommand Subscribe stopped by signal.")
			return
		}
	}
}

func (t *taskExecutor) Stop() {
	log.Println("⏳  taskExecutor Stopping...")
	close(t.quit)

	if t.config.Kafka.Enabled {
		t.consumer.Stop()
	} else {

		if err := t.pubSub.Close(); err != nil {
			log.Printf("❌  pubsub err: %v", err)
		}
	}
	log.Println("👍 taskExecutor Stopped.")
}
