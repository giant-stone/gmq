package main

import (
	"context"
	"flag"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/gutil"

	"github.com/giant-stone/gmq/gmq"
)

func main() {
	const defaultDsn = "redis://127.0.0.1:6379/0"
	var dsn string
	flag.StringVar(&dsn, "d", defaultDsn, "data source name of redis")
	flag.Parse()

	// Initialize glogging with formatted output, log rotation, and log levels.
	glogging.Init([]string{"stdout"}, "debug")

	broker, err := gmq.NewBrokerRedis(dsn, "")
	gutil.ExitOnErr(err)

	ctx := context.Background()

	// Consumer.
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

	// Producer.
	// cronSpec accepts standard five-field cron expressions and descriptors.
	//  [cron](https://en.wikipedia.org/wiki/Cron)
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
