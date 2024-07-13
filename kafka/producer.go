package kafka

// You only need **one** of these per package!
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"github.com/IBM/sarama"
	"github.com/ehsaniara/delay-box/config"
	"log"
	"os"
	"strings"
)

//counterfeiter:generate . SyncProducer
type SyncProducer interface {
	SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error)
}

//counterfeiter:generate . AsyncProducer
type AsyncProducer interface {
	sarama.AsyncProducer
}

// indicator to see if sarama.SyncProducer are exist
var _ SyncProducer = (sarama.SyncProducer)(nil)

type producer struct {
	producer sarama.SyncProducer
	brokers  string
}

func NewProducer(config *config.Config) (SyncProducer, func()) {

	if len(config.Kafka.Brokers) == 0 {
		log.Fatal("brokers host is missing")
	}

	p := &producer{
		producer: nil,
		brokers:  config.Kafka.Brokers,
	}
	c := sarama.NewConfig()
	c.Producer.RequiredAcks = sarama.NoResponse
	c.Producer.Return.Successes = true
	c.Producer.Retry.Max = 5
	c.Producer.Partitioner = sarama.NewRandomPartitioner
	var err error
	p.producer, err = sarama.NewSyncProducer(strings.Split(p.brokers, ","), c)
	if err != nil {
		log.Printf("❌  Failed to open Kafka producer: %s", err)
		os.Exit(1)
	}
	return p.producer, p.Stop
}

func (p *producer) Stop() {
	log.Println("⏳  Producer Stopping...")
	if err := p.producer.Close(); err != nil {
		log.Printf("❌  Failed to close Kafka producer cleanly: %v", err)
	}
	log.Println("👍 Producer Stopped!")
}
