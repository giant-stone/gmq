package main

import (
	"context"
	"flag"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/gutil"

	"github.com/giant-stone/gmq/gmq"
)

func workIntervalFunc() time.Duration {
	return time.Second * time.Duration(3)
}

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

	slowQueueName := "checkout"
	srv := gmq.NewServer(ctx, broker, &gmq.Config{Logger: glogging.Sugared,
		QueueConfigs: map[string]*gmq.QueueCfg{
			// Map queue names to their configurations.
			slowQueueName: gmq.NewQueueCfg(
				gmq.OptQueueWorkerNum(1),                    // Use one worker for this queue.
				gmq.OptWorkerWorkInterval(workIntervalFunc), // Consume one message every three seconds.
			),
		},
	})
	mux := gmq.NewMux()

	// Simulate a message producer in a goroutine.
	cli, err := gmq.NewClientRedis(dsn, "")
	gutil.ExitOnErr(err)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				{
					// Use gmq.DefaultQueueName when no queue name is specified.
					cli.Enqueue(ctx, &gmq.Msg{Payload: []byte(`{"data":"hello world"}`)})
					cli.Enqueue(ctx, &gmq.Msg{Payload: []byte(`{"fullURI":"example.sample/123"}`)}, gmq.OptQueueName(slowQueueName))
					time.Sleep(time.Millisecond * 500)
				}
			}
		}
	}()

	// Register a consumer with mux, similar to routing in a web framework.
	// Use the queue name as the pattern and gmq.HandlerFunc as the handler.
	mux.Handle(gmq.DefaultQueueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		glogging.Sugared.Debugf("consume id=%s queue=%s payload=%s", msg.GetId(), msg.GetQueue(), string(msg.GetPayload()))
		return nil
	}))

	mux.Handle(slowQueueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		glogging.Sugared.Debugf("consume id=%s queue=%s payload=%s", msg.GetId(), msg.GetQueue(), string(msg.GetPayload()))
		return nil
	}))

	if err := srv.Run(mux); err != nil {
		glogging.Sugared.Fatal("srv.Run ", err)
	}
	glogging.Sugared.Debug("gmq server started")
	select {}
}
