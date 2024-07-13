package kafka

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"github.com/ehsaniara/scheduler/config"
	"github.com/ehsaniara/scheduler/interfaces"
	"log"
	"strings"
	"time"
)

// You only need **one** of these per package!
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

func NewConsumerGroup(config *config.Config) interfaces.ConsumerGroup {
	version, _ := sarama.ParseKafkaVersion(sarama.DefaultVersion.String())

	c := sarama.NewConfig()
	c.Version = version
	c.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	c.Consumer.Offsets.Initial = sarama.OffsetOldest

	brokers := strings.Split(config.Kafka.Brokers, `,`)

	// Setting up the consumer group
	cg, err := sarama.NewConsumerGroup(brokers, config.Kafka.Group, c)
	if err != nil {
		log.Fatalf("❌  Error creating consumer group: %v", err)
	}
	return cg
}

//counterfeiter:generate . Consumer
type Consumer interface {
	Start(ctx context.Context)
	Stop()
}

type consumer struct {
	topics          []string
	quit            chan struct{}
	dispatchMessage func(message *sarama.ConsumerMessage)
	cg              interfaces.ConsumerGroup
}

func NewConsumer(
	topics []string,
	consumerGroup interfaces.ConsumerGroup,
	dispatchMessage func(message *sarama.ConsumerMessage),
) Consumer {
	return &consumer{
		topics:          topics,
		quit:            make(chan struct{}),
		dispatchMessage: dispatchMessage,
		cg:              consumerGroup,
	}
}

func (c *consumer) Stop() {
	log.Printf("👍 Stopping Kafka Consumer Group started on topic: %v ...\n", c.topics)
	close(c.quit)
}

func (c *consumer) Start(ctx context.Context) {

	handler := consumerGroupHandler{
		dispatchMessage: c.dispatchMessage,
		ready:           make(chan bool, 1),
	}

	go func() {
		defer func() {
			if err := c.cg.Close(); err != nil {
				log.Fatalf("❌  Error closing consumer group: %v", err)
			}
			close(handler.ready)
		}()

		for {
			select {
			case err, ok := <-c.cg.Errors():
				if ok {
					log.Printf("Error: %s", err)
				}
			case <-c.quit:
				log.Println("Received stop signal")
				return
			default:
				err := c.cg.Consume(ctx, c.topics, handler)
				if err != nil {
					if errors.Is(err, sarama.ErrClosedConsumerGroup) || errors.Is(err, context.Canceled) {
						log.Println("Consumer group closed or context canceled")
						return
					}
					log.Printf("❌  Error consuming: %v\n", err)
					return
				}
				// Check if context was cancelled, signaling a graceful shutdown
				if ctx.Err() != nil {
					log.Println("🤯 Context canceled")
					return
				}
			}
		}

	}()
	<-handler.ready
	time.Sleep(time.Second * 5)
	log.Printf("✔️ Kafka Consumer Group started on topic: %v\n", c.topics)
}

type consumerGroupHandler struct {
	dispatchMessage func(message *sarama.ConsumerMessage)
	ready           chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.ready <- true
	for message := range claim.Messages() {
		h.dispatchMessage(message)
		session.MarkMessage(message, "")
	}
	return nil
}
