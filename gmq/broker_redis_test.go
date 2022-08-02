package gmq_test

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/giant-stone/go/glogging"
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
		glogging.Sugared.Fatal("srv.Run ", err)
	}
	glogging.Sugared.Debug("gmq server started")

	// pause and resume correctly
	time.Sleep(time.Second)
	err = srv.Pause("default")
	require.NoError(t, err, "srv.Pause")
	// repeated operation
	time.Sleep(time.Second)
	err = srv.Pause("default")
	require.Error(t, err, "srv.Pause")

	err = srv.Resume("default")
	require.NoError(t, err, "srv.Resume")
	err = srv.Resume("default")
	require.Error(t, err, "srv.Resume")
}
