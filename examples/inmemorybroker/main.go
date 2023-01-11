package main

import (
	"context"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/gutil"
)

func main() {
	glogging.Init([]string{"stdout"}, "debug")

	maxBytes := 1024 * 1024 * 32
	broker, err := gmq.NewBrokerInMemory(
		&gmq.BrokerInMemoryOpts{MaxBytes: int64(maxBytes)},
	)
	gutil.ExitOnErr(err)

	gmq.NewClientFromBroker(broker)

	srv := gmq.NewServer(context.Background(), broker, nil)
	mux := gmq.NewMux()
	mux.Handle("test", gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		return nil
	}))

	err = srv.Run(mux)
	gutil.ExitOnErr(err)

	select {}
}
