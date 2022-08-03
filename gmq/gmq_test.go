package gmq_test

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/go/glogging"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

var (
	dsnRedis    string
	TestNum     int
	mux         sync.Mutex
	dsnNoSelect string
)

func init() {
	TestNum = 1
	glogging.Init([]string{"stdout"}, "debug")
	flag.StringVar(&dsnRedis, "dsnRedis", "redis://localhost:6379/14", "redis data source name for testing")
	flag.StringVar(&dsnNoSelect, "dsnNoSelect", "redis://localhost:6379/", "redis data source name for testing, automatically select database number")
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
	return
}

func setupWithClient(tb testing.TB) (broker gmq.Broker, rdb *redis.Client) {
	tb.Helper()
	rdb = getClient(tb)
	broker, err := gmq.NewBrokerFromRedisClient(rdb)
	require.NoError(tb, err, "gmq.NewBrokerFromRedisClient")
	err = rdb.FlushDB(context.Background()).Err()
	require.NoError(tb, err, "cli.FlushDB")
	return
}

func getClient(tb testing.TB) (cli *redis.Client) {
	mux.Lock()
	TestNum++
	mux.Unlock()
	fmt.Println(TestNum)
	opts, err := redis.ParseURL(dsnNoSelect + strconv.Itoa(TestNum))
	require.NoError(tb, err, "redis.ParseURL")
	cli = redis.NewClient(opts)
	return
}

func msgPattern(qname string) string {
	return gmq.Namespace + ":" + qname + ":msg:*"
}
