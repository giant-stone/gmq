package gmq_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/gtime"
	"github.com/giant-stone/go/gutil"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
)

func TestPauseAndResume(t *testing.T) {
	broker := getTestBroker(t)
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
	cli, err := gmq.NewClient(dsnRedis)
	gutil.ExitOnErr(err)
	payload := []byte(fmt.Sprintf("{\"data\": \"Msg Fromm TestPauseAndResume\"}"))
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

	// 设置消息消费者，mux 类似于 web 框架中常用的多路复用路由处理，
	// 消费消息以队列名为 pattern，handler 为 gmq.HandlerFunc 类型函数
	countProcessed := 0
	countFailed := 0
	count := 0
	mux.Handle(testQueueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		count++
		if count%2 == 0 {
			countFailed++
			glogging.Sugared.Debugf("Failed"+strconv.Itoa(countFailed), msg.GetId(), msg.GetQueue(), string(msg.GetPayload()))
			return errors.New("this is a failure test for test queue")
		} else {
			countProcessed++
			glogging.Sugared.Debugf("Processed"+strconv.Itoa(countProcessed), msg.GetId(), msg.GetQueue(), string(msg.GetPayload()))
			return nil
		}

	}))

	if err := srv.Run(mux); err != nil {
		require.NoError(t, err, "srv.Run")
	}

	// wait for a while
	time.Sleep(time.Second)
	// pause and resume invalid queue
	require.ErrorIs(t, srv.Pause("queueNotExist"), gmq.ErrInvalidQueue)
	require.ErrorIs(t, srv.Resume("queueNotExist"), gmq.ErrInvalidQueue)

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
	ProcessedBeforePause := dailyStats.Processed
	FailedBeforePause := dailyStats.Failed

	// repeated operation for pause
	err = srv.Pause(testQueueName)
	require.Error(t, err, "srv.Pause")

	// check if there is any msg processed
	time.Sleep(time.Millisecond * 1000)
	dailyStats, err = broker.GetStatsByDate(ctx, date)
	require.NoError(t, err, "srv.Resume")
	ProcessedAfterPause := dailyStats.Processed
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
	ProcessedAfterResume := dailyStats.Processed
	FailedAfterResume := dailyStats.Failed
	require.NotZero(t, ProcessedAfterResume-ProcessedAfterPause,
		fmt.Sprintf("srv.Resume ProcessedAfterResume: %d, ProcessedAfterPause: %d", ProcessedAfterResume, ProcessedAfterPause))
	require.NotZero(t, FailedAfterResume-FailedAfterPause,
		fmt.Sprintf("srv.Resume FailedAfterResume: %d, FailedAfterPause: %d", FailedAfterResume, FailedAfterPause))

}

func TestFail(t *testing.T) {
	broker := getTestBroker(t)
	rdb := getTestClient(t)
	defer broker.Close()

	testQueueName := "QueueTestFail"
	srv := gmq.NewServer(context.Background(), broker, &gmq.Config{QueueCfgs: map[string]*gmq.QueueCfg{
		// 队列名 - 队列配置
		testQueueName: gmq.NewQueueCfg(
			gmq.OptQueueWorkerNum(2), // 配置限制队列只有一个 worker
		),
	}})
	mux := gmq.NewMux()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli, err := gmq.NewClientFromBroker(broker)
	gutil.ExitOnErr(err)

	msgNum := 200
	wg := sync.WaitGroup{}
	wg.Add(msgNum)
	payload := []byte(fmt.Sprintf("{\"data\": \"Msg Fromm TestFail\"}"))
	go func() {
		for i := 0; i < msgNum; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				{
					cli.Enqueue(ctx, &gmq.Msg{Payload: payload, Queue: testQueueName})
				}
			}
		}
	}()

	// 设置消息消费者，mux 类似于 web 框架中常用的多路复用路由处理，
	// 消费消息以队列名为 pattern，handler 为 gmq.HandlerFunc 类型函数
	countFailed := 0
	countProcessed := 0
	mux.Handle(testQueueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		glogging.Sugared.Debugf("consume id=%s queue=%s payload=%s", msg.GetId(), msg.GetQueue(), string(msg.GetPayload()))
		// 防止队列为空自旋
		time.Sleep(10 * time.Millisecond)
		wg.Done()
		if rand.Intn(3) <= 2 {
			countFailed++
			return errors.New("this is a failure test for default queue")
		} else {
			countProcessed++
			return nil
		}
	}))
	if err := srv.Run(mux); err != nil {
		require.NoError(t, err, "srv.Run")
	}

	wg.Wait()
	// 等待最后的任务完成
	time.Sleep(time.Second)
	require.Equal(t, msgNum, countFailed+countProcessed)
	date := gtime.UnixTime2YyyymmddUtc(time.Now().Unix())
	dailyStats, err := broker.GetStatsByDate(ctx, date)
	require.NoError(t, err, "srv.GetStatsByDate")
	ProcessedAfterPause := dailyStats.Processed
	FailedAfterPause := dailyStats.Failed

	require.Zero(t, ProcessedAfterPause-int64(countProcessed), "Processed Queue Msg Records")
	require.Zero(t, FailedAfterPause-int64(countFailed), "Failed Queue Msg Records")

	msgs, err := rdb.Keys(ctx, msgPattern(testQueueName)).Result()
	require.Equal(t, countFailed, len(msgs), fmt.Sprintf("there should be %d records in cache, but got %d", countFailed, len(msgs)))
	require.NoError(t, err, "redis")
	n, err := rdb.LLen(ctx, gmq.NewKeyQueueFailed(gmq.Namespace, testQueueName)).Result()
	require.Equal(t, int64(countFailed), n, "Failed Records Num")
	require.NoError(t, err, "redis")
	for i := range msgs {
		state, err := rdb.HGet(ctx, msgs[i], "state").Result()
		require.NoError(t, err)
		require.Equal(t, "failed", state)
	}
}

func workIntervalFunc() time.Duration {
	return time.Millisecond * 200
}

func TestDeleteAgo(t *testing.T) {
	broker := getTestBroker(t)
	rdb := getTestClient(t)
	defer broker.Close()
	mux := gmq.NewMux()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slowQueueName := "seckill"
	testQueueName := "QueueTestDeleteAgo"
	srv := gmq.NewServer(ctx, broker, &gmq.Config{Logger: glogging.Sugared,
		QueueCfgs: map[string]*gmq.QueueCfg{
			// 队列名 - 队列配置
			slowQueueName: gmq.NewQueueCfg(
				gmq.OptQueueWorkerNum(1),                    // 配置限制队列只有一个 worker
				gmq.OptWorkerWorkInterval(workIntervalFunc), // 配置限制队列消费间隔为每 3 秒从队列取一条消息
			),
			testQueueName: gmq.NewQueueCfg(
				gmq.OptQueueWorkerNum(2), //
			),
		},
	})

	// 设置消息消费者，mux 类似于 web 框架中常用的多路复用路由处理，
	// 消费消息以队列名为 pattern，handler 为 gmq.HandlerFunc 类型函数
	mux.Handle(testQueueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		glogging.Sugared.Debugf("consume id=%s queue=%s payload=%s", msg.GetId(), msg.GetQueue(), string(msg.GetPayload()))
		if rand.Intn(2) == 1 {
			return errors.New("this is a failure test for default queue")
		} else {
			return nil
		}

	}))

	mux.Handle(slowQueueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		glogging.Sugared.Debugf("consume id=%s queue=%s payload=%s", msg.GetId(), msg.GetQueue(), string(msg.GetPayload()))
		if rand.Intn(2) == 1 {
			return errors.New("this is a failure test for slow queue")
		} else {
			return nil
		}
	}))

	if err := srv.Run(mux); err != nil {
		require.NoError(t, err, "srv.Run")
	}
	cutoff := time.Now().Add(-time.Second * 1000)
	created := cutoff.UnixMilli()
	// notcutoff := int64(time.Now().Second()) + 1000
	payload := fmt.Sprintf("{\"data\": \"Msg Fromm TestFail\"}")
	for i := 0; i < 10; i++ {
		msg := &gmq.Msg{Payload: []byte("Outdated msg: " + payload), Id: uuid.NewString(), Queue: testQueueName}
		addMsgAtProcessing(t, ctx, rdb, msg, []int64{created, created})
		msg = &gmq.Msg{Payload: []byte("Outdated msg: " + payload), Id: uuid.NewString(), Queue: slowQueueName}
		addMsgAtProcessing(t, ctx, rdb, msg, []int64{created, created})

		msg = &gmq.Msg{Payload: []byte("Outdated msg: " + payload), Id: uuid.NewString(), Queue: testQueueName}
		addMsgAtFailed(t, ctx, rdb, msg, []int64{created, created, created})
		msg = &gmq.Msg{Payload: []byte("Outdated msg: " + payload), Id: uuid.NewString(), Queue: slowQueueName}
		addMsgAtFailed(t, ctx, rdb, msg, []int64{created, created, created})
	}

	// 检查队列消息是否成功删除
	broker.DeleteAgo(ctx, testQueueName, int64(time.Now().Second()))
	count, err := rdb.LLen(ctx, gmq.NewKeyQueueProcessing(gmq.Namespace, testQueueName)).Result()
	require.NoError(t, err)
	require.Equal(t, 0, int(count))

	count, err = rdb.LLen(ctx, gmq.NewKeyQueueFailed(gmq.Namespace, testQueueName)).Result()
	require.NoError(t, err)
	require.Equal(t, 0, int(count))

	broker.DeleteAgo(ctx, slowQueueName, int64(time.Now().Second()))
	count, err = rdb.LLen(ctx, gmq.NewKeyQueueProcessing(gmq.Namespace, slowQueueName)).Result()
	require.NoError(t, err)
	require.Equal(t, 0, int(count))

	count, err = rdb.LLen(ctx, gmq.NewKeyQueueFailed(gmq.Namespace, slowQueueName)).Result()
	require.NoError(t, err)
	require.Equal(t, 0, int(count))

	created = time.Now().UnixMilli()
	msg := &gmq.Msg{Payload: []byte("Available msg: " + payload), Id: uuid.NewString(), Queue: testQueueName}
	addMsgAtProcessing(t, ctx, rdb, msg, []int64{created, created})

	count, err = rdb.LLen(ctx, gmq.NewKeyQueueProcessing(gmq.Namespace, testQueueName)).Result()
	require.NoError(t, err)
	require.Equal(t, 1, int(count))

	ret, err := rdb.Keys(ctx, msgPattern(testQueueName)).Result()
	require.NoError(t, err)
	require.Equal(t, 1, len(ret))

	// wait for a while
	after := 500 * time.Millisecond
	time.Sleep(after)
	broker.DeleteAgo(ctx, testQueueName, int64(after.Seconds()))

	count, err = rdb.LLen(ctx, gmq.NewKeyQueueProcessing(gmq.Namespace, testQueueName)).Result()
	require.NoError(t, err)
	require.Equal(t, 0, int(count))

	ret, err = rdb.Keys(ctx, msgPattern(testQueueName)).Result()
	require.NoError(t, err)
	require.Equal(t, 0, len(ret))
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
