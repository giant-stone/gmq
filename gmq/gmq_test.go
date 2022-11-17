//go:build redis
// +build redis

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
	defaultDsnRedis = "redis://localhost:6379/14?dial_timeout=1s&read_timeout=1s&max_retries=1"
)

var (
	universalBroker gmq.Broker
	universalCli    *redis.Client
)

// setup returns a redis broker for testing
func setup(tb testing.TB) (broker gmq.Broker) {
	tb.Helper()

	dsnRedis := os.Getenv("REDIS")
	if dsnRedis == "" {
		dsnRedis = defaultDsnRedis
	}
	loglevel := os.Getenv("LOGLEVEL")
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
	universalCli = cli
	universalBroker = broker
	return
}

func getTestBroker(t testing.TB) gmq.Broker {
	setup(t)
	return universalBroker
}

func getTestClient(t testing.TB) *redis.Client {
	setup(t)
	err := universalCli.FlushDB(context.Background()).Err()
	require.NoError(t, err, "cli.FlushDB")
	return universalCli
}

func msgPattern(qname string) string {
	return gmq.Namespace + ":" + qname + ":msg:*"
}
