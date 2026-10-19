package gmq_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/grand"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/gmq/internal/test_util"
)

var (
	defaultLogLevel = "debug"
)

var (
	universalBrokerRedis gmq.Broker
	universalRedisClient *redis.Client
)

// setupBrokerRedis returns a redis broker for testing
func setupBrokerRedis(tb testing.TB) (broker gmq.Broker) {
	testCtx := test_util.Context(tb)
	tb.Helper()

	dsnRedis := os.Getenv("GMQ_RDS")
	if dsnRedis == "" {
		tb.Skip("skip all redis broker related tests because of env GMQ_RDS not set")
	}

	logLevel := os.Getenv("GMQ_LOG_LEVEL")
	if logLevel == "" {
		logLevel = defaultLogLevel
	}
	glogging.Init([]string{"stderr"}, glogging.Loglevel(logLevel))

	opts, err := redis.ParseURL(dsnRedis)
	require.NoError(tb, err, "redis.ParseURL")

	cli := redis.NewClient(opts)
	err = cli.FlushDB(testCtx).Err()
	require.NoError(tb, err, "cli.FlushDB")

	broker, err = gmq.NewBrokerFromRedisClient(cli, "")
	require.NoError(tb, err, "gmq.NewBrokerFromRedisClient")

	err = cli.FlushDB(testCtx).Err()
	require.NoError(tb, err, "cli.FlushDB")

	universalRedisClient = cli
	universalBrokerRedis = broker
	return universalBrokerRedis
}

func getTestBrokerRedis(t testing.TB) gmq.Broker {
	return setupBrokerRedis(t)
}

func getTestRedisClient(t testing.TB) *redis.Client {
	setupBrokerRedis(t)
	return universalRedisClient
}

func TestBrokerRedis_PauseAndResume(t *testing.T) {
	const (
		messageCount   = 4
		pollInterval   = 10 * time.Millisecond
		waitTimeout    = 5 * time.Second
		pausedDuration = 100 * time.Millisecond
	)
	testCtx := test_util.Context(t)
	ctx, cancel := context.WithCancel(testCtx)
	broker := getTestBrokerRedis(t)
	defer broker.Close()
	defer cancel()
	queueName := grand.String(10)
	srv := gmq.NewServer(ctx, broker, &gmq.Config{
		RestIfNoMsg: pollInterval,
		MsgMaxTTL:   time.Minute,
		QueueConfigs: map[string]*gmq.QueueCfg{
			queueName: gmq.NewQueueCfg(gmq.OptQueueWorkerNum(1)),
		},
	})
	mux := gmq.NewMux()
	handled := 0
	mux.Handle(queueName, gmq.HandlerFunc(func(context.Context, gmq.IMsg) error {
		handled++
		if handled%2 == 0 {
			return errors.New("handler failed")
		}
		return nil
	}))
	require.NoError(t, srv.Run(mux))
	require.NoError(t, srv.Pause(grand.String(10)))
	require.NoError(t, srv.Resume(grand.String(10)))
	require.NoError(t, srv.Pause(queueName))
	require.Error(t, srv.Pause(queueName))

	for i := 0; i < messageCount; i++ {
		_, err := broker.Enqueue(ctx, &gmq.Msg{Queue: queueName, Payload: []byte("example")})
		require.NoError(t, err)
	}
	date := time.Now().Format("2006-01-02")
	require.Never(t, func() bool {
		stats, err := broker.GetStatsByDate(ctx, []string{queueName}, date)
		return err != nil || stats.Total != 0
	}, pausedDuration, pollInterval, "paused queue must not process messages")
	queues, err := broker.GetStats(ctx)
	require.NoError(t, err)
	require.Len(t, queues, 1)
	require.EqualValues(t, messageCount, queues[0].Pending)
	require.Zero(t, queues[0].Processing)

	require.NoError(t, srv.Resume(queueName))
	require.NoError(t, srv.Resume(queueName))
	require.Eventually(t, func() bool {
		stats, err := broker.GetStatsByDate(ctx, []string{queueName}, date)
		return err == nil && stats.Completed == messageCount/2 && stats.Failed == messageCount/2
	}, waitTimeout, pollInterval, "resumed queue must complete and fail the queued messages")
}

func TestBrokerRedis_Enqueue(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerEnqueue(t, broker)
}

func TestBrokerRedis_GetMsg(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerGetMsg(t, broker)
}

func TestBrokerRedis_Dequeue(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerDequeue(t, broker)
}

func TestBrokerRedis_DeleteMsg(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerDeleteMsg(t, broker)
}

func TestBrokerRedis_DeleteQueue(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerDeleteQueue(t, broker)
}

func TestBrokerRedis_DeleteAgo(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerDeleteAgo(t, broker)
}

func TestBrokerRedis_Complete(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerComplete(t, broker)
}

func TestBrokerRedis_Fail(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerFail(t, broker)
}

func TestBrokerRedis_ListMsg(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerListMsg(t, broker)
}

func TestBrokerRedis_ListFailed(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerListFailed(t, broker)
}

func TestBrokerRedis_ListFailedMaxItems(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerListFailedMaxItems(t, broker)
}

func TestBrokerRedis_GetStats(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerGetStats(t, broker)
}

func TestBrokerRedis_GetStatsByDate(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerGetStatsByDate(t, broker)
}

func TestBrokerRedis_AutoDeduplicateMsgByDefault(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerAutoDeduplicateMsgByDefault(t, broker)
}

func TestBrokerRedis_AutoDeduplicateFailedMsg(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerAutoDeduplicateFailedMsg(t, broker)
}

func TestBrokerRedis_AutoDeduplicateCompletedMsg(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	gmq.CheckBrokerAutoDeduplicateCompletedMsg(t, broker)
}

func TestBrokerRedis_ClientEnqueue(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	testClient_Enqueue(t, broker)
}

func TestBrokerRedis_ClientDequeue(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	testClient_Dequeue(t, broker)
}

func TestBrokerRedis_ClientEnqueueOptQueueName(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	testClient_EnqueueOptQueueName(t, broker)
}

func TestBrokerRedis_ClientEnqueueDuplicatedMsg(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	testClient_EnqueueDuplicatedMsg(t, broker)
}

func TestBrokerRedis_ClientEnqueueOptUniqueIn(t *testing.T) {
	test_util.Context(t)
	broker := getTestBrokerRedis(t)
	testClient_EnqueueOptUniqueIn(t, broker)
}
