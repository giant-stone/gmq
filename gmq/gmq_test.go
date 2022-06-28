package gmq_test

import (
	"context"
	"flag"
	"testing"

	"github.com/giant-stone/go/glogging"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
)

var (
	dsnRedis string
)

func init() {
	glogging.Init([]string{"stdout"}, "debug")

	flag.StringVar(&dsnRedis, "dsnRedis", "redis://localhost:6379/14", "redis data source name for testing")
}

// setup returns a redis broker for testing
func setup(tb testing.TB) (broker gmq.Broker) {
	tb.Helper()

	opts, err := redis.ParseURL(dsnRedis)
	require.NoError(tb, err, "redis.ParseURL")

	cli := redis.NewClient(opts)
	err = cli.FlushDB(context.Background()).Err()
	require.NoError(tb, err, "cli.FlushDB")

	broker, err = gmq.NewBrokerFromRedisClient(cli)
	require.NoError(tb, err, "gmq.NewBrokerFromRedisClient")
	return broker
}
