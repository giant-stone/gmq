# Custom consume rate

Each queue defaults to `runtime.NumCPU()` workers with no deliberate interval.
Use one worker and a callback to pace a queue:

```go
const requestInterval = 3 * time.Second

server := gmq.NewServer(ctx, broker, &gmq.Config{
    QueueConfigs: map[string]*gmq.QueueCfg{
        "checkout": gmq.NewQueueCfg(
            gmq.OptQueueWorkerNum(1),
            gmq.OptWorkerWorkInterval(func() time.Duration {
                return requestInterval
            }),
        ),
    },
})
```

Register a `checkout` handler and enqueue with `Msg.Queue: "checkout"` or `gmq.OptQueueName("checkout")`.
See the full [queue configuration example](../examples/queue_config/main.go).

The processor measures handler runtime, calls the callback after the handler returns,
and waits for the positive remainder after recording success or failure.
A three-second interval and one-second handler leave roughly two seconds of waiting.
A handler taking longer than the interval adds no wait. Broker overhead can increase spacing.
Returning `context.DeadlineExceeded` skips the interval wait in the current processor.
Empty queues instead wait for `Config.RestIfNoMsg`, defaulting to one second.

The callback can return different durations on each invocation.
With multiple workers it can run concurrently; synchronize shared state using a mutex or atomics.

Pacing applies per worker, queue, and consumer process. Two workers or processes can consume concurrently.
Use one worker in one process for a single paced stream. This is not a global quota,
an exact execution schedule, or a retry delay.
