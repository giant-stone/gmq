package gmq_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/gtime"
	"github.com/giant-stone/go/gutil"
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
	broker := setup(t)
	defer broker.Close()

	srv := gmq.NewServer(context.Background(), broker, nil)
	mux := gmq.NewMux()

	ctx := context.Background()
	cli, err := gmq.NewClient(dsnRedis)
	rdb := getClient(t)
	gutil.ExitOnErr(err)

	msgNum := 100
	go func() {
		for i := 0; i < msgNum; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				{
					// 如果不指定队列名，gmq 默认使用 gmq.DefaultQueueName
					cli.Enqueue(ctx, &gmq.Msg{Payload: []byte("hello world:" + strconv.Itoa(i))})
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
		if rand.Intn(2) == 1 {
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

	time.Sleep(time.Second * 5) // 等待所有消息处理完毕
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
	require.Equal(t, int64(countFailed), n, "Failed Records Num")
	require.NoError(t, err, "redis")
	for i := range msgs {
		state, err := rdb.HGet(ctx, msgs[i], "state").Result()
		require.NoError(t, err)
		require.Equal(t, "failed", state)
	}
}
