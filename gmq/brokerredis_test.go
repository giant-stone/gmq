package gmq_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/gtime"
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

	universalRedisClient = cli
	universalBrokerRedis = broker
	return universalBrokerRedis
}

func getTestBrokerRedis(t testing.TB) gmq.Broker {
	return setupBrokerRedis(t)
}

func getTestRedisClient(t testing.TB) *redis.Client {
	setupBrokerRedis(t)
	err := universalRedisClient.FlushDB(context.Background()).Err()
	require.NoError(t, err, "cli.FlushDB")
	return universalRedisClient
}

func TestGmq_PauseAndResume(t *testing.T) {
	broker := getTestBrokerRedis(t)
	defer broker.Close()

	testQueueName := "QueueTestPauseAndResume"
	srv := gmq.NewServer(context.Background(), broker, &gmq.Config{QueueCfgs: map[string]*gmq.QueueCfg{
		// 队列名 - 队列配置
		testQueueName: gmq.NewQueueCfg(
			gmq.OptQueueWorkerNum(1), // 配置限制队列只有一个 worker
		),
	}})
	mux := gmq.NewMux()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli, err := gmq.NewClientFromBroker(broker)
	gutil.ExitOnErr(err)
	payload := []byte("{\"data\": \"Msg Fromm TestPauseAndResume\"}")
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				{
					cli.Enqueue(ctx, &gmq.Msg{Payload: payload, Queue: testQueueName})
					time.Sleep(time.Millisecond * 100)
				}
			}
		}
	}()

	countProcessed := 0
	countFailed := 0
	count := 0
	mux.Handle(testQueueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		count++
		if count%2 == 0 {
			countFailed++
			return errors.New("this is a failure test for test queue")
		} else {
			countProcessed++
			return nil
		}

	}))

	if err := srv.Run(mux); err != nil {
		require.NoError(t, err, "srv.Run")
	}

	// wait for a while
	time.Sleep(time.Second)
	// pause and resume invalid queue
	require.NoError(t, srv.Pause("queueNotExist"))
	require.NoError(t, srv.Resume("queueNotExist"))

	// pause and resume correctly
	time.Sleep(time.Millisecond * 500)
	err = srv.Pause(testQueueName)
	require.NoError(t, err, "srv.Pause")
	log.Printf("Queue %s Paused", testQueueName)
	// wait for msgs under processing complete
	time.Sleep(time.Millisecond * 500)

	// records the processed and failed msg numbers
	date := gtime.UnixTime2YyyymmddUtc(time.Now().Unix())
	dailyStats, err := broker.GetStatsByDate(ctx, date)
	require.NoError(t, err, "srv.Pause")
	ProcessedBeforePause := dailyStats.Completed
	FailedBeforePause := dailyStats.Failed

	// repeated operation for pause
	err = srv.Pause(testQueueName)
	require.Error(t, err, "srv.Pause")

	// check if there is any msg processed
	time.Sleep(time.Millisecond * 1000)
	dailyStats, err = broker.GetStatsByDate(ctx, date)
	require.NoError(t, err, "srv.Resume")
	ProcessedAfterPause := dailyStats.Completed
	FailedAfterPause := dailyStats.Failed

	require.Zero(t, ProcessedAfterPause-(ProcessedBeforePause),
		fmt.Sprintf("srv.Pause ProcessedAfterPause: %d, ProcessedBeforePause: %d", ProcessedAfterPause, ProcessedBeforePause))
	require.Zero(t, FailedAfterPause-FailedBeforePause,
		fmt.Sprintf("srv.Pause FailedAfterPause: %d, FailedBeforePause: %d", FailedAfterPause, FailedBeforePause))

	err = srv.Resume(testQueueName)
	require.NoError(t, err, "srv.Resume")
	log.Printf("Queue %s Resumed", testQueueName)
	// repeated operation for resume
	err = srv.Resume(testQueueName)
	require.Error(t, err, "srv.Resume")

	// check if the worker resumes to comsume
	time.Sleep(time.Millisecond * 1000)
	dailyStats, err = broker.GetStatsByDate(ctx, date)
	require.NoError(t, err, "srv.Resume")
	ProcessedAfterResume := dailyStats.Completed
	FailedAfterResume := dailyStats.Failed
	require.NotZero(t, ProcessedAfterResume-ProcessedAfterPause,
		fmt.Sprintf("srv.Resume ProcessedAfterResume: %d, ProcessedAfterPause: %d", ProcessedAfterResume, ProcessedAfterPause))
	require.NotZero(t, FailedAfterResume-FailedAfterPause,
		fmt.Sprintf("srv.Resume FailedAfterResume: %d, FailedAfterPause: %d", FailedAfterResume, FailedAfterPause))

}

func TestGmq_DeleteAgo(t *testing.T) {
	msg := GenerateNewMsg()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := getTestRedisClient(t)
	require.NotNil(t, cli)
	broker, err := gmq.NewBrokerFromRedisClient(cli)
	require.NoError(t, err)

	now := time.Now()
	broker.SetClock(gmq.NewSimulatedClock(now))
	defer broker.Close()

	restIfNoMsg := time.Millisecond * time.Duration(10)
	msgMaxTTL := time.Millisecond * time.Duration(100)
	srv := gmq.NewServer(ctx, broker, &gmq.Config{MsgMaxTTL: msgMaxTTL, RestIfNoMsg: restIfNoMsg})
	mux := gmq.NewMux()

	mux.Handle(msg.Queue, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		return errors.New("error")
	}))

	err = srv.Run(mux)
	require.NoError(t, err, "srv.Run")

	_, err = broker.Enqueue(ctx, msg)
	require.NoError(t, err)

	// wait consumer done
	time.Sleep(restIfNoMsg * 2)

	// check it first time
	queueStats, err := broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, len(queueStats), 1)

	queueStat := queueStats[0]
	require.Equal(t, msg.Queue, queueStat.Name)
	require.Equal(t, int64(1), queueStat.Total)
	require.Equal(t, int64(0), queueStat.Pending)
	require.Equal(t, int64(0), queueStat.Waiting)
	require.Equal(t, int64(0), queueStat.Processing)
	require.Equal(t, int64(0), queueStat.Completed)
	require.Equal(t, int64(1), queueStat.Failed)

	keys, err := broker.ListMsg(ctx, msg.Queue, gmq.MsgStateFailed, 0, -1)
	require.NoError(t, err)
	require.Equal(t, msg.Id, keys[0])

	// wait cleaner done
	time.Sleep(msgMaxTTL)

	// check it second time
	queueStats, err = broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, len(queueStats), 1)

	queueStat = queueStats[0]
	require.Equal(t, msg.Queue, queueStat.Name)
	require.Equal(t, int64(0), queueStat.Total)
	require.Equal(t, int64(0), queueStat.Pending)
	require.Equal(t, int64(0), queueStat.Waiting)
	require.Equal(t, int64(0), queueStat.Processing)
	require.Equal(t, int64(0), queueStat.Completed)
	require.Equal(t, int64(0), queueStat.Failed)

	_, err = broker.ListMsg(ctx, msg.Queue, gmq.MsgStateFailed, 0, -1)
	require.ErrorIs(t, gmq.ErrNoMsg, err)
}

// KEYS[1] -> gmq:<queuename>:processing
// KEYS[2] -> gmq:<queuename>:msg:<MsgId>
// ARGV[1] -> state
// ARGV[2] -> payload
// ARGV[3] -> msgId
// ARGV[4] -> create
// ARGV[5] -> processedat
var scriptAddMsgProcessing = redis.NewScript(`
redis.call("LPUSH", KEYS[1], ARGV[3])
redis.call("HSET", KEYS[2],
           "payload", ARGV[2],
           "state",   ARGV[1],
           "created", ARGV[4],
           "processedat", ARGV[5])
return 0
`)

func addMsgAtProcessing(t *testing.T, ctx context.Context, cli *redis.Client, msg gmq.IMsg, timeLines []int64) {
	keys := []string{
		gmq.NewKeyQueueProcessing(gmq.Namespace, msg.GetQueue()),
		gmq.NewKeyMsgDetail(gmq.Namespace, msg.GetQueue(), msg.GetId()),
	}
	args := []interface{}{
		"processing",
		msg.GetPayload(),
		msg.GetId(),
		timeLines[0],
		timeLines[1],
	}
	resI, err := scriptAddMsgProcessing.Run(ctx, cli, keys, args).Result()
	require.NoError(t, err)
	rt, ok := resI.(int64)
	require.True(t, ok)
	require.NotEqual(t, gmq.LuaReturnCodeError, rt)
}

// KEYS[1] -> gmq:<queuename>:failed
// KEYS[2] -> gmq:<queuename>:msg:<MsgId>
// ARGV[1] -> state
// ARGV[2] -> payload
// ARGV[3] -> msgId
// ARGV[4] -> created
// ARGV[5] -> processedat
// ARGV[6] -> dieat

var scriptAddMsgFailed = redis.NewScript(`
redis.call("LPUSH", KEYS[1], ARGV[3])
redis.call("HSET", KEYS[2],
           "payload", ARGV[2],
           "state",   ARGV[1],
           "created", ARGV[4],
           "processedat", ARGV[5],
           "dieat", ARGV[6])
return 0
`)

func addMsgAtFailed(t *testing.T, ctx context.Context, cli *redis.Client, msg gmq.IMsg, timeLines []int64) {
	keys := []string{
		gmq.NewKeyQueueFailed(gmq.Namespace, msg.GetQueue()),
		gmq.NewKeyMsgDetail(gmq.Namespace, msg.GetQueue(), msg.GetId()),
	}
	args := []interface{}{
		"failed",
		msg.GetPayload(),
		msg.GetId(),
		timeLines[0],
		timeLines[1],
		timeLines[2],
	}
	resI, err := scriptAddMsgFailed.Run(ctx, cli, keys, args).Result()
	require.NoError(t, err)
	rt, ok := resI.(int64)
	require.True(t, ok)
	require.NotEqual(t, gmq.LuaReturnCodeError, rt)
}

func TestBrokerRedis_GetStatsWeekly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lastRecord := 15
	now := time.Now().AddDate(0, 0, -lastRecord)
	broker := getTestBrokerRedis(t)
	rdb := getTestRedisClient(t)
	broker.SetClock(gmq.NewSimulatedClock(now))
	var err error
	// 生成记录
	queueList := []string{"default"}
	for _, queue := range queueList {
		_, err := rdb.SAdd(ctx, gmq.NewKeyQueueList(), queue).Result()
		require.NoError(t, err)
	}
	step := len(queueList)
	msgFailed, msgProcessed := make([]int64, (lastRecord+1)*step), make([]int64, (lastRecord+1)*step)
	periods := 7

	for i := 0; i < (lastRecord+1)*step; {
		for _, queue := range queueList {
			msgFailed[i] = int64(rand.Intn(1000))
			msgProcessed[i] = int64(rand.Intn(1000))
			_, err = rdb.Set(ctx, gmq.NewKeyDailyStatFailed(gmq.Namespace, queue, gtime.UnixTime2YyyymmddUtc(now.Unix())), msgFailed[i], 0).Result()
			require.NoError(t, err)
			_, err = rdb.Set(ctx, gmq.NewKeyDailyStatCompleted(gmq.Namespace, queue, gtime.UnixTime2YyyymmddUtc(now.Unix())), msgProcessed[i], 0).Result()
			require.NoError(t, err)
			i++
		}
		now = now.AddDate(0, 0, 1)
	}

	var sumFailed, sumProcessed int64 = 0, 0
	for idx := 0; idx <= periods*step; idx += step {
		for tmp := range queueList {
			sumFailed += msgFailed[idx+tmp]
			sumProcessed += msgProcessed[idx+tmp]
		}
	}

	for j := 0; j < lastRecord-periods; j++ {
		now := time.Now().AddDate(0, 0, periods-lastRecord+j)
		broker.SetClock(gmq.NewSimulatedClock(now))
		rsStat, err := broker.GetStatsWeekly(ctx)
		require.NoError(t, err)
		info := fmt.Sprintf("brokder.GetStatsWeekly date: %s", gtime.UnixTime2YyyymmddUtc(now.Unix()))

		totalCompleted := int64(0)
		totalFailed := int64(0)
		for _, item := range rsStat {
			totalCompleted += item.Completed
			totalFailed += item.Failed
		}

		require.Equal(t, sumProcessed, totalCompleted, info)
		require.Equal(t, sumFailed, totalFailed, info)

		// iter
		for idx := range queueList {
			sumFailed -= msgFailed[idx+j]
			sumFailed += msgFailed[idx+j+(periods+1)*step]
			sumProcessed -= msgProcessed[idx+j]
			sumProcessed += msgProcessed[idx+j+(periods+1)*step]
		}
	}

}
