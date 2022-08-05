package gmq_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/gtime"
	"github.com/giant-stone/go/gutil"
	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
)

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
