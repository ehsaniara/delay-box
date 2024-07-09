package main

import (
	"context"
	"github.com/ehsaniara/scheduler/config"
	"github.com/ehsaniara/scheduler/core"
	"github.com/ehsaniara/scheduler/httpserver"
	"github.com/ehsaniara/scheduler/kafka"
	"github.com/ehsaniara/scheduler/storage"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	log.Printf("🛰 Starting scheduler app \n")
	ctx, cancel := context.WithCancel(context.Background())

	c := config.GetConfig()
	config.Print(c)

	// Redis localhost:6379, secret
	s, redisClientClose := storage.NewRedisClient(ctx, c)

	// RR Partition Producer
	producer, producerClose := kafka.NewProducer(c)

	// starting the scheduler app
	scheduler := core.NewScheduler(ctx, s, producer, c)

	//start all consumers
	consumerGroup := kafka.NewConsumerGroup(c)
	dispatchSchedulerConsumer := kafka.NewConsumer(
		strings.Split(c.Kafka.SchedulerTopic, `,`),
		consumerGroup,
		scheduler.Dispatcher,
	)
	dispatchSchedulerConsumer.Start(ctx)

	//start server
	stopServer := httpserver.NewServer(ctx, nil, scheduler, c)
	log.Println("🚀 scheduler is ready!")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	//wait until os stop signal arrives
	keepRunning := true
	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("🖐️ terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("🖐️ terminating: via signal")
			keepRunning = false
		}
	}
	log.Println("⏳  Stopping all services...")
	cancel()
	scheduler.Stop()
	dispatchSchedulerConsumer.Stop()
	redisClientClose()
	producerClose()
	stopServer()

	log.Println("💔 Bye!")
}
