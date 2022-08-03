// 不要运行file tests， 多个test并行会导致错误
package gmq_test

import (
	"context"
	"errors"
	"fmt"
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
	broker := setup(t)
	defer broker.Close()

	srv := gmq.NewServer(context.Background(), broker, nil)
	mux := gmq.NewMux()

	ctx := context.Background()
	cli, err := gmq.NewClient(dsnRedis)
	gutil.ExitOnErr(err)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				{
					// 如果不指定队列名，gmq 默认使用 gmq.DefaultQueueName
					cli.Enqueue(ctx, &gmq.Msg{Payload: []byte(`{"data":"hello world"}`)})
					time.Sleep(time.Millisecond * 200)
				}
			}
		}
	}()

	// 设置消息消费者，mux 类似于 web 框架中常用的多路复用路由处理，
	// 消费消息以队列名为 pattern，handler 为 gmq.HandlerFunc 类型函数
	mux.Handle(gmq.DefaultQueueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		glogging.Sugared.Debugf("consume id=%s queue=%s payload=%s", msg.GetId(), msg.GetQueue(), string(msg.GetPayload()))
		if rand.Intn(2) == 1 {
			return errors.New("this is a failure test for default queue")
		} else {
			return nil
		}

	}))

	if err := srv.Run(mux); err != nil {
		require.NoError(t, err, "srv.Run")
	}

	// pause and resume invalid queue
	require.ErrorIs(t, srv.Pause("queueNotExist"), gmq.ErrInvalidQueue)
	require.ErrorIs(t, srv.Resume("queueNotExist"), gmq.ErrInvalidQueue)

	// pause and resume correctly
	time.Sleep(time.Second)
	err = srv.Pause("default")
	require.NoError(t, err, "srv.Pause")
	date := gtime.UnixTime2YyyymmddUtc(time.Now().Unix())
	dailyStats, err := broker.GetStatsByDate(ctx, date)
	require.NoError(t, err, "srv.Pause")
	ProcessedBeforePause := dailyStats.Processed
	FailedBeforePause := dailyStats.Failed

	// repeated operation for pause
	time.Sleep(time.Second)
	err = srv.Pause("default")
	require.Error(t, err, "srv.Pause")

	// check if there is any msg are processed
	dailyStats, err = broker.GetStatsByDate(ctx, date)
	require.NoError(t, err, "srv.Resume")
	ProcessedAfterPause := dailyStats.Processed
	FailedAfterPause := dailyStats.Failed

	require.Zero(t, ProcessedAfterPause-ProcessedBeforePause, "srv.Pause")
	require.Zero(t, FailedAfterPause-FailedBeforePause, "srv.Pause")

	err = srv.Resume("default")
	require.NoError(t, err, "srv.Resume")
	// repeated operation for resume
	err = srv.Resume("default")
	require.Error(t, err, "srv.Resume")

	// check if the worker resumes to comsume
	time.Sleep(time.Second)
	dailyStats, err = broker.GetStatsByDate(ctx, date)
	require.NoError(t, err, "srv.Resume")
	ProcessedAfterResume := dailyStats.Processed
	FailedAfterResume := dailyStats.Failed
	require.NotZero(t, ProcessedAfterResume-ProcessedAfterPause, "srv.Resume")
	require.NotZero(t, FailedAfterResume-FailedAfterPause, "srv.Resume")

}

func TestFail(t *testing.T) {
	broker, rdb := setupWithClient(t)
	defer broker.Close()

	srv := gmq.NewServer(context.Background(), broker, nil)
	mux := gmq.NewMux()

	ctx := context.Background()
	cli, err := gmq.NewClientFromBroker(broker)
	gutil.ExitOnErr(err)

	msgNum := 200
	wg := sync.WaitGroup{}
	wg.Add(msgNum)
	go func() {
		for i := 0; i < msgNum; i++ {
			// 如果不指定队列名，gmq 默认使用 gmq.DefaultQueueName
			select {
			case <-ctx.Done():
				return
			default:
				{
					// 如果不指定队列名，gmq 默认使用 gmq.DefaultQueueName
					cli.Enqueue(ctx, &gmq.Msg{Payload: []byte(`{"data":"hello world"}`)})
				}
			}
		}
	}()

	// 设置消息消费者，mux 类似于 web 框架中常用的多路复用路由处理，
	// 消费消息以队列名为 pattern，handler 为 gmq.HandlerFunc 类型函数
	countFailed := 0
	countProcessed := 0
	mux.Handle(gmq.DefaultQueueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
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

	// 验证是否存在错误队列
	wg.Wait()
	// 等待最后的任务完成
	time.Sleep(time.Second)
	require.Equal(t, msgNum, countFailed+countProcessed)
	date := gtime.UnixTime2YyyymmddUtc(time.Now().Unix())
	dailyStats, err := broker.GetStatsByDate(ctx, date)
	require.NoError(t, err, "srv.Resume")
	ProcessedAfterPause := dailyStats.Processed
	FailedAfterPause := dailyStats.Failed

	require.Zero(t, ProcessedAfterPause-int64(countProcessed), "Processed Queue Msg Records")
	require.Zero(t, FailedAfterPause-int64(countFailed), "Failed Queue Msg Records")

	msgs, err := rdb.Keys(ctx, msgPattern("default")).Result()
	require.Equal(t, countFailed, len(msgs), fmt.Sprintf("there should be %d records in cache, but got %d", countFailed, len(msgs)))
	require.NoError(t, err, "redis")
	n, err := rdb.ZCard(ctx, gmq.NewKeyQueueFailed(gmq.Namespace, "default")).Result()
	// 与gmq.maxFailedQueueLength一致
	if countFailed >= gmq.MaxFailedQueueLength-1 {
		require.Equal(t, int64(gmq.MaxFailedQueueLength-1), n, "Failed Records Num")
	} else {
		require.Equal(t, int64(countFailed), n, "Failed Records Num")
	}
	require.NoError(t, err, "redis")
	for i := range msgs {
		state, err := rdb.HGet(ctx, msgs[i], "state").Result()
		require.NoError(t, err)
		require.Equal(t, "failed", state)
	}
}

func workIntervalFunc() time.Duration {
	return time.Second * time.Duration(1)
}

func TestDeleteAgo(t *testing.T) {
	broker, rdb := setupWithClient(t)
	defer broker.Close()
	mux := gmq.NewMux()

	ctx := context.Background()
	slowQueueName := "seckill"
	srv := gmq.NewServer(ctx, broker, &gmq.Config{Logger: glogging.Sugared,
		QueueCfgs: map[string]*gmq.QueueCfg{
			// 队列名 - 队列配置
			slowQueueName: gmq.NewQueueCfg(
				gmq.OptQueueWorkerNum(1),                    // 配置限制队列只有一个 worker
				gmq.OptWorkerWorkInterval(workIntervalFunc), // 配置限制队列消费间隔为每 3 秒从队列取一条消息
			),
		},
	})

	// 设置消息消费者，mux 类似于 web 框架中常用的多路复用路由处理，
	// 消费消息以队列名为 pattern，handler 为 gmq.HandlerFunc 类型函数
	mux.Handle(gmq.DefaultQueueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
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
	for i := 0; i < 10; i++ {
		msg := &gmq.Msg{Payload: []byte(fmt.Sprintf("Outdated msg" + strconv.Itoa(i))), Id: uuid.NewString(), Queue: "default"}
		addMsgAtProcessing(t, ctx, rdb, msg, []int64{created, created})
		msg = &gmq.Msg{Payload: []byte(fmt.Sprintf("Outdated msg" + strconv.Itoa(i))), Id: uuid.NewString(), Queue: "seckill"}
		addMsgAtProcessing(t, ctx, rdb, msg, []int64{created, created})

		msg = &gmq.Msg{Payload: []byte(fmt.Sprintf("Outdated msg" + strconv.Itoa(i))), Id: uuid.NewString(), Queue: "default"}
		addMsgAtFailed(t, ctx, rdb, msg, []int64{created, created, created})
		msg = &gmq.Msg{Payload: []byte(fmt.Sprintf("Outdated msg" + strconv.Itoa(i))), Id: uuid.NewString(), Queue: "seckill"}
		addMsgAtFailed(t, ctx, rdb, msg, []int64{created, created, created})
	}

	// 检查队列消息是否成功删除
	broker.DeleteAgo(ctx, gmq.DefaultQueueName, int64(time.Now().Second()))
	count, err := rdb.LLen(ctx, gmq.NewKeyQueueProcessing(gmq.Namespace, gmq.DefaultQueueName)).Result()
	require.NoError(t, err)
	require.Equal(t, 0, int(count))

	count, err = rdb.LLen(ctx, gmq.NewKeyQueueFailed(gmq.Namespace, gmq.DefaultQueueName)).Result()
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
	msg := &gmq.Msg{Payload: []byte("Available msg"), Id: uuid.NewString(), Queue: "default"}
	addMsgAtProcessing(t, ctx, rdb, msg, []int64{created, created})

	count, err = rdb.LLen(ctx, gmq.NewKeyQueueProcessing(gmq.Namespace, gmq.DefaultQueueName)).Result()
	require.NoError(t, err)
	require.Equal(t, 1, int(count))

	ret, err := rdb.Keys(ctx, msgPattern(gmq.DefaultQueueName)).Result()
	require.NoError(t, err)
	require.Equal(t, 1, len(ret))

	// wait for a while
	after := 3 * time.Second
	time.Sleep(after)
	broker.DeleteAgo(ctx, gmq.DefaultQueueName, int64(after.Seconds()))

	count, err = rdb.LLen(ctx, gmq.NewKeyQueueProcessing(gmq.Namespace, gmq.DefaultQueueName)).Result()
	require.NoError(t, err)
	require.Equal(t, 0, int(count))

	ret, err = rdb.Keys(ctx, msgPattern(gmq.DefaultQueueName)).Result()
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

// KEYS[1] -> gmq:<queuename>:processing
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
