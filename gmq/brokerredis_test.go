package gmq_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/grand"
	"github.com/giant-stone/go/gutil"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
)

var (
	defaultLoglevel = "debug"
)

var (
	universalBrokerRedis gmq.Broker
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

	err = cli.FlushDB(context.Background()).Err()
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
	broker := getTestBrokerRedis(t)
	defer broker.Close()

	queueName := grand.String(10)

	restIfNoMsg := time.Millisecond * time.Duration(10)
	msgMaxTTL := time.Minute
	glogging.Init([]string{"stderr"}, "warn")
	srv := gmq.NewServer(context.Background(), broker, &gmq.Config{
		RestIfNoMsg: restIfNoMsg,
		MsgMaxTTL:   msgMaxTTL,
		QueueCfgs: map[string]*gmq.QueueCfg{
			queueName: gmq.NewQueueCfg(
				gmq.OptQueueWorkerNum(1),
			),
		}})
	mux := gmq.NewMux()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli, err := gmq.NewClientFromBroker(broker)
	gutil.ExitOnErr(err)
	payload := []byte(grand.String(10))
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				{
					cli.Enqueue(ctx, &gmq.Msg{Payload: payload, Queue: queueName})
					time.Sleep(restIfNoMsg)
				}
			}
		}
	}()

	countCompleted := 0
	countFailed := 0
	countTotal := 0
	mux.Handle(queueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		countTotal++
		if countTotal%2 == 0 {
			countFailed++
			return errors.New("something wrong")
		} else {
			countCompleted++
			return nil
		}

	}))

	err = srv.Run(mux)
	require.NoError(t, err)

	// wait for a while
	time.Sleep(restIfNoMsg * 2)
	// pause and resume invalid queue
	require.NoError(t, srv.Pause(grand.String(10)))
	require.NoError(t, srv.Resume(grand.String(10)))

	// pause and resume correctly
	time.Sleep(restIfNoMsg * 2)
	err = srv.Pause(queueName)
	require.NoError(t, err)
	// wait for msgs under processing complete
	time.Sleep(restIfNoMsg * 2)

	// records the Completed and failed msg numbers
	todayYYYYMMDD := time.Now().Format("2006-01-02")
	dailyStats, err := broker.GetStatsByDate(ctx, todayYYYYMMDD)
	require.NoError(t, err)
	CompletedBeforePause := dailyStats.Completed
	FailedBeforePause := dailyStats.Failed

	// repeated operation for pause
	err = srv.Pause(queueName)
	require.Error(t, err)

	// check if there is any msg Completed
	time.Sleep(restIfNoMsg * 2)
	dailyStats, err = broker.GetStatsByDate(ctx, todayYYYYMMDD)
	require.NoError(t, err)
	CompletedAfterPause := dailyStats.Completed
	FailedAfterPause := dailyStats.Failed

	require.Zero(t, CompletedAfterPause-(CompletedBeforePause),
		fmt.Sprintf("srv.Pause CompletedAfterPause: %d, CompletedBeforePause: %d", CompletedAfterPause, CompletedBeforePause))
	require.Zero(t, FailedAfterPause-FailedBeforePause,
		fmt.Sprintf("srv.Pause FailedAfterPause: %d, FailedBeforePause: %d", FailedAfterPause, FailedBeforePause))

	err = srv.Resume(queueName)
	require.NoError(t, err)
	// repeated operation for resume
	err = srv.Resume(queueName)
	require.NoError(t, err)

	// check if the worker resumes to comsume
	time.Sleep(restIfNoMsg * 2)
	dailyStats, err = broker.GetStatsByDate(ctx, todayYYYYMMDD)
	require.NoError(t, err)
	CompletedAfterResume := dailyStats.Completed
	FailedAfterResume := dailyStats.Failed
	require.NotZero(t, CompletedAfterResume-CompletedAfterPause,
		fmt.Sprintf("srv.Resume CompletedAfterResume: %d, CompletedAfterPause: %d", CompletedAfterResume, CompletedAfterPause))
	require.NotZero(t, FailedAfterResume-FailedAfterPause,
		fmt.Sprintf("srv.Resume FailedAfterResume: %d, FailedAfterPause: %d", FailedAfterResume, FailedAfterPause))

}

func TestBrokerRedis_Enqueue(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_Enqueue(t, broker)
}

func TestBrokerRedis_GetMsg(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_GetMsg(t, broker)
}

func TestBrokerRedis_Dequeue(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_Dequeue(t, broker)
}

func TestBrokerRedis_DeleteMsg(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_DeleteMsg(t, broker)
}

func TestBrokerRedis_DeleteQueue(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_DeleteQueue(t, broker)
}

func TestBrokerRedis_DeleteAgo(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_DeleteAgo(t, broker)
}

func TestBrokerRedis_Complete(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_Complete(t, broker)
}

func TestBrokerRedis_Fail(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_Fail(t, broker)
}

func TestBrokerRedis_ListMsg(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_ListMsg(t, broker)
}

func TestBrokerRedis_ListFailed(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_ListFailed(t, broker)
}

func TestBrokerRedis_ListFailedMaxItems(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_ListFailedMaxItems(t, broker)
}

func TestBrokerRedis_GetStats(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_GetStats(t, broker)
}

func TestBrokerRedis_GetStatsByDate(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_GetStatsByDate(t, broker)
}

func TestBrokerRedis_AutoDeduplicateMsgByDefault(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_AutoDeduplicateMsgByDefault(t, broker)
}

func TestBrokerRedis_AutoDeduplicateFailedMsg(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_AutoDeduplicateFailedMsg(t, broker)
}

func TestBrokerRedis_AutoDeduplicateCompletedMsg(t *testing.T) {
	broker := getTestBrokerRedis(t)
	gmq.TestBroker_AutoDeduplicateCompletedMsg(t, broker)
}

func TestBrokerRedis_ClientEnqueue(t *testing.T) {
	broker := getTestBrokerRedis(t)
	testClient_Enqueue(t, broker)
}

func TestBrokerRedis_ClientDequeue(t *testing.T) {
	broker := getTestBrokerRedis(t)
	testClient_Dequeue(t, broker)
}

func TestBrokerRedis_ClientEnqueueOptQueueName(t *testing.T) {
	broker := getTestBrokerRedis(t)
	testClient_EnqueueOptQueueName(t, broker)
}

func TestBrokerRedis_ClientEnqueueDuplicatedMsg(t *testing.T) {
	broker := getTestBrokerRedis(t)
	testClient_EnqueueDuplicatedMsg(t, broker)
}

func TestBrokerRedis_ClientEnqueueOptUniqueIn(t *testing.T) {
	broker := getTestBrokerRedis(t)
	testClient_EnqueueOptUniqueIn(t, broker)
}

func TestBrokerRedis_ClientEnqueueOptTypeIgnoreUnique(t *testing.T) {
	broker := getTestBrokerRedis(t)
	testClient_EnqueueOptTypeIgnoreUnique(t, broker)
}
