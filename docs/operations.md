<!-- cspell:ignore syscall -->

# Scheduling and operations

## Recurring jobs

The scheduler enqueues messages; a separately started server consumes them.

```go
scheduler := gmq.NewScheduler(gmq.SchedulerParams{
    Ctx: ctx,
    Broker: broker,
    Location: time.UTC,
})
jobID, err := scheduler.Register("*/5 * * * *", &gmq.Msg{
    Queue: "reports",
    Payload: []byte(`{"kind":"summary"}`),
})
if err != nil {
    return err
}
if err := scheduler.Run(); err != nil {
    return err
}
defer scheduler.Shutdown()
// Call scheduler.Unregister(jobID) to remove the registration.
_ = jobID
```

The current constructor uses robfig/cron's default five-field parser and descriptors such as `@hourly`
and `@every 10s`. Unlike the old wiki's claim, it does not enable a Quartz or seconds-field parser.
The underlying interval scheduler does not support intervals shorter than one second.
UTC is the default location; set `Location` for calendar schedules in another timezone.
Registrations are process-local and must be recreated after restart.
Multiple schedulers can enqueue the same work; coordinate ownership.
A fixed message ID can suppress later occurrences until uniqueness expires;
leave `Msg.Id` empty when each occurrence should enqueue fresh work.

## Middleware

Register middleware before starting workers:

```go
mux.Use(gmq.LoggingElapsed)
```

Custom middleware has type `func(gmq.IHandler) gmq.IHandler`.
Wrap `next.ProcessMsg(ctx, msg)` in a `gmq.HandlerFunc` to add instrumentation or validation.
Middleware runs in registration order. Preserve errors so failures reach the broker.

## Pause and resume

Use `server.Pause("reports")` and `server.Resume("reports")`, checking returned errors.
Pause stops new claims without canceling active handlers.
Server controls only act on registered queues.

## Shutdown

Use `signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)` to receive termination signals.
Wait for the returned context's `Done` channel, stop producers, call `scheduler.Shutdown()`,
then call `server.Shutdown()`.
Handlers must honor cancellation so shutdown can finish. Server shutdown waits for workers and closes the broker.
There is no built-in shutdown deadline. Cancellation can prevent recording final completion or failure;
inspect unfinished processing messages after an interrupted operation.

## Redis management CLI

Build using `make buildBin` on macOS/Linux or `build.bat` on Windows.
The direct command is `go build -trimpath -tags timetzdata -o gmqcli.bin ./gmq/cmd/gmqcli`.
Use `gmqcli.exe` as the output name on Windows.

```sh
./gmqcli.bin -d redis://127.0.0.1:6379/0 -stat
./gmqcli.bin -d redis://127.0.0.1:6379/0 -listQueue
./gmqcli.bin -d redis://127.0.0.1:6379/0 -q reports -list -s failed
./gmqcli.bin -d redis://127.0.0.1:6379/0 -pause reports
./gmqcli.bin -d redis://127.0.0.1:6379/0 -resume reports
```
