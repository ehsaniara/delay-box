package storage

// You only need **one** of these per package!
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"context"
	"github.com/ehsaniara/scheduler/config"
	"github.com/ehsaniara/scheduler/interfaces"
	"github.com/redis/go-redis/v9"
	"log"
)

// indicator to see if *redis.Client are exist
var _ interfaces.RedisDBClient = (*redis.Client)(nil)

// NewRedisClient returns RedisDBClient to use in application, and close func
// rdb parameter is to test this file and pass mocks
func NewRedisClient(ctx context.Context, config *config.Config, rdb interfaces.RedisDBClient) (TaskStorage, func()) {

	if len(config.Storage.RedisHost) == 0 {
		log.Fatal("redisHost is missing")
	}

	client := &taskRedisClient{
		config: config,
		rdb:    rdb,
	}

	//if it's not for testing mocks
	if rdb == nil {
		client.SetUp(ctx, config.Storage.RedisHost, config.Storage.RedisPass, config.Storage.RedisDb)
	}

	return client, func() {
		log.Printf("⏳ Redis connection pool Closing...\n")
		// it's not test mock
		if rdb != nil {
			err := rdb.Close()
			if err != nil {
				log.Printf("Error Close taskRedisClient:%v \n", err)
			}
		}
		log.Printf("👍 Redis connection pool Closed!\n")
	}
}

type taskRedisClient struct {
	rdb    interfaces.RedisDBClient
	config *config.Config
}

func (c *taskRedisClient) SetUp(ctx context.Context, redisHost, redisPass string, database int) {

	if len(redisHost) == 0 {
		log.Fatal("redisHost is missing")
	}

	log.Printf("⏳ Redis Setting up... \n")
	c.rdb = redis.NewClient(&redis.Options{
		Addr:     redisHost,
		Password: redisPass, // secret
		DB:       database,  // use default DB
		Network:  "tcp",
	})
	log.Printf("✔️ Redis Started on Host: %v\n", c.rdb)
	pong, err := c.rdb.Ping(ctx).Result()
	log.Printf("✔️ Redis Ping: %v\n", pong)

	if err != nil {
		log.Fatalf("Redis Connection Failed: %v\n", c.rdb)
	}
}
