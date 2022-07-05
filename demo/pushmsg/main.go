package main

import (
	"context"
	"flag"
	"time"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/gutil"
)

func main() {
	const defaultDsn = "redis://127.0.0.1:6379/0"
	var dsn string
	flag.StringVar(&dsn, "d", defaultDsn, "data source name of redis")
	flag.Parse()

	// 初始化 glogging 打印日志，[glogging](https://github.com/giant-stone/go#custom-logging) 将集成格式化、自动切割、日志分级，满足大部分服务 99% 以上场景
	glogging.Init([]string{"stdout"}, "debug")

	broker, err := gmq.NewBrokerRedis(dsn)
	gutil.ExitOnErr(err)

	ctx := context.Background()
	srv := gmq.NewServer(ctx, broker, &gmq.Config{Logger: glogging.Sugared})
	mux := gmq.NewMux()

	// 用一个子协程模拟实现消息队列生产者
	cli, err := gmq.NewClient(dsn)
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
					time.Sleep(time.Millisecond * 500)
				}
			}
		}
	}()

	// 设置消息消费者，mux 类似于 web 框架中常用的多路复用路由处理，
	// 消费消息以队列名为 pattern，handler 为 gmq.HandlerFunc 类型函数
	mux.Handle(gmq.DefaultQueueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		glogging.Sugared.Debugf("consume id=%s queue=%s payload=%s", msg.GetId(), msg.GetQueue(), string(msg.GetPayload()))
		return nil
	}))

	if err := srv.Run(mux); err != nil {
		glogging.Sugared.Fatal("srv.Run ", err)
	}
	glogging.Sugared.Debug("gmq server started")
	select {}
}
