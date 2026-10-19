# Getting started

Use a supported stable Go release. The module declares Go 1.20 as its minimum language version.
Inside a Go module, install the library:

```sh
go get github.com/giant-stone/gmq@latest
```

This complete program uses SQLite without an external service. It handles one message and exits.
Save as `main.go`, then run `go run .`. Use a fresh `queue.db` for this demo.

```go
package main

import (
	"context"
	"log"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/go/glogging"
)

func main() {
	glogging.Init([]string{"stdout"}, "info")
	ctx := context.Background()
	broker, err := gmq.NewBrokerSQLite("queue.db")
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Close()
	client, err := gmq.NewClientFromBroker(broker)
	if err != nil {
		log.Fatal(err)
	}
	_, err = client.Enqueue(ctx, &gmq.Msg{
		Queue:   "greetings",
		Payload: []byte(`{"message":"hello world"}`),
	})
	if err != nil {
		log.Fatal(err)
	}

	handled := make(chan struct{}, 1)
	mux := gmq.NewMux()
	mux.Handle("greetings", gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) error {
		log.Printf("queue=%s id=%s payload=%s", msg.GetQueue(), msg.GetId(), msg.GetPayload())
		handled <- struct{}{}
		return nil
	}))
	server := gmq.NewServer(ctx, broker, nil)
	if err := server.Run(mux); err != nil {
		log.Fatal(err)
	}
	<-handled
	server.Shutdown()
}
```

Initialize `glogging` before using the default logger, or supply an initialized logger through `Config.Logger`.
`Run` starts background workers and returns; keep the process alive until work or shutdown completes.
`Shutdown` waits for workers and closes the broker. SQLite tolerates the deferred second close.

For Redis, use `gmq.NewBrokerRedis("redis://127.0.0.1:6379/0", "")`.
For an isolated in-memory queue, use `gmq.NewBrokerInMemory(nil)`.
Separate producers and consumers must connect to the same persistent broker.

An empty queue name defaults to `gmq.DefaultQueueName`; `OptQueueName` overrides `Msg.Queue`.
Register each queue with `mux.Handle` before starting the server.
Mux supports prefix dispatch, but the server creates processors for registered patterns,
not every queue whose name starts with a pattern.

See [examples/push_msg](../examples/push_msg) for a continuous Redis producer and consumer.
