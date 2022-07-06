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

	// 消费者
	srv := gmq.NewServer(ctx, broker, &gmq.Config{Logger: glogging.Sugared})
	mux := gmq.NewMux()
	mux.Handle(gmq.DefaultQueueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		glogging.Sugared.Debugf("consume id=%s queue=%s payload=%s", msg.GetId(), msg.GetQueue(), string(msg.GetPayload()))
		return nil
	}))

	if err := srv.Run(mux); err != nil {
		glogging.Sugared.Fatal("srv.Run ", err)
	}

	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		glogging.Sugared.Fatal("time.LoadLocation ", err)
	}

	scheduler := gmq.NewScheduler(gmq.SchedulerParams{
		Ctx:      ctx,
		Broker:   broker,
		Logger:   glogging.Sugared,
		Location: loc,
	})

	// 生产者
	// cronSpec 支持两种常见格式
	//  [cron](https://en.wikipedia.org/wiki/Cron)
	//  [quartz-scheduler](http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/tutorial-lesson-06.html)
	cronSpec := "@every 500ms"
	_, err = scheduler.Register(cronSpec, &gmq.Msg{Payload: []byte(`{"msg":"hello"}`)})
	if err != nil {
		glogging.Sugared.Error(err)
	}

	if err := scheduler.Run(); err != nil {
		glogging.Sugared.Fatal("scheduler.Run ", err)
	}

	glogging.Sugared.Debug("scheduler started")
	select {}
}
