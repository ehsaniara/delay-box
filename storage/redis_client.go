package storage

import (
	"context"
	"github.com/ehsaniara/scheduler/interfaces"
	"github.com/redis/go-redis/v9"
	"log"
)

type redisClient struct {
	rdb interfaces.RedisDBClient
}

// indicator to see if *redis.Client are exist
var _ interfaces.RedisDBClient = (*redis.Client)(nil)

// NewRedisClient returns RedisDBClient to use in application, and close func
func NewRedisClient(ctx context.Context, redisHost, redisPass string, database int) (interfaces.RedisDBClient, func()) {

	if len(redisHost) == 0 {
		log.Fatal("redisHost is missing")
	}

	client := redisClient{}
	client.SetUp(ctx, redisHost, redisPass, database)

	return client.rdb, client.Close
}

func (c *redisClient) SetUp(ctx context.Context, redisHost, redisPass string, database int) {

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

func (c *redisClient) Close() {
	log.Printf("⏳ Redis connection pool Closing...\n")
	err := c.rdb.Close()
	if err != nil {
		log.Printf("Error Close redisClient:%v \n", err)
	}
	log.Printf("👍 Redis connection pool Closed!\n")
}
