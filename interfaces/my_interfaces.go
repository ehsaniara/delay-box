package interfaces

// You only need **one** of these per package!
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
)

// RedisDBClient is an interface that abstracts the *redis.Client methods so we can use mock later.
// or use the built-in interface: SortedSetCmdable
//
//counterfeiter:generate . RedisDBClient
type RedisDBClient interface {
	ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	ZCard(ctx context.Context, key string) *redis.IntCmd
	Ping(ctx context.Context) *redis.StatusCmd
	Options() *redis.Options
	Close() error
	Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
}

//counterfeiter:generate . ConsumerGroup
type ConsumerGroup interface {
	sarama.ConsumerGroup
}

//counterfeiter:generate . ConsumerGroupSession
type ConsumerGroupSession interface {
	sarama.ConsumerGroupSession
}

//counterfeiter:generate . ConsumerGroupClaim
type ConsumerGroupClaim interface {
	sarama.ConsumerGroupClaim
}
