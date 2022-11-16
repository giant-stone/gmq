package main

import (
	"context"
	"flag"

	"github.com/giant-stone/go/gutil"

	"github.com/giant-stone/gmq/examples/crawl/impl"
	"github.com/giant-stone/gmq/gmq"
)

func main() {
	const defaultDsn = "redis://127.0.0.1:6379/14"
	var dsn string
	flag.StringVar(&dsn, "d", defaultDsn, "data source name of redis")
	flag.Parse()
	broker, err := gmq.NewBrokerRedis(dsn)
	gutil.ExitOnErr(err)
	impl.RunCrawlClient(context.Background(), broker)
	ctx := context.Background()
	impl.RunCrawlServer(ctx, broker)
	select {}
}
