package worker

// You only need **one** of these per package!
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"os/exec"
)

//counterfeiter:generate . TaskExecutor
type TaskExecutor interface {
	ExecuteCommand(message *sarama.ConsumerMessage)
}

type taskExecutor struct {
	shellPath string
}

func NewTaskExecutor() TaskExecutor {
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

	log.Printf("Using shell: %s\n", shellPath)

	return &taskExecutor{
		shellPath: shellPath,
	}
}

func (t *taskExecutor) ExecuteCommand(message *sarama.ConsumerMessage) {
	log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s\n", string(message.Value), message.Timestamp, message.Topic)

	taskType := ""

	for _, header := range message.Headers {
		if string(header.Key) == "taskType" {
			taskType = string(header.Value)
		}
	}

	if taskType == "SHELL_CMD" {
		cmd := exec.Command(t.shellPath, "-c", string(message.Value))

		// Run the command and capture the output
		output, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Println("Error executing command:", err)
			return
		}

		// Print the output
		fmt.Println(string(output))
	}
}
