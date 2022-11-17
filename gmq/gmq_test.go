package gmq_test

import (
	"context"
	"os"
	"testing"

	"github.com/giant-stone/go/glogging"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
)

var (
	defaultLoglevel = "debug"
)

var (
	universalBroker      gmq.Broker
	universalRedisClient *redis.Client
)

// setupBrokerRedis returns a redis broker for testing
func setupBrokerRedis(tb testing.TB) (broker gmq.Broker) {
	tb.Helper()

	dsnRedis := os.Getenv("GMQ_RDS")
	if dsnRedis == "" {
		tb.Skip("skip all redis broker releated tests because of env GMQ_RDS not set")
	}

	loglevel := os.Getenv("GMQ_LOGLEVEL")
	if loglevel == "" {
		loglevel = defaultLoglevel
	}
	glogging.Init([]string{"stderr"}, glogging.Loglevel(loglevel))

	opts, err := redis.ParseURL(dsnRedis)
	require.NoError(tb, err, "redis.ParseURL")

	cli := redis.NewClient(opts)
	err = cli.FlushDB(context.Background()).Err()
	require.NoError(tb, err, "cli.FlushDB")

	broker, err = gmq.NewBrokerFromRedisClient(cli)
	require.NoError(tb, err, "gmq.NewBrokerFromRedisClient")

	universalRedisClient = cli
	universalBroker = broker
	return universalBroker
}

func getTestBroker(t testing.TB) gmq.Broker {
	setupBrokerRedis(t)
	return universalBroker
}

func getTestRedisClient(t testing.TB) *redis.Client {
	setupBrokerRedis(t)
	err := universalRedisClient.FlushDB(context.Background()).Err()
	require.NoError(t, err, "cli.FlushDB")
	return universalRedisClient
}

func msgPattern(qname string) string {
	return gmq.Namespace + ":" + qname + ":msg:*"
}
