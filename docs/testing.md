# Testing with gomock

Inject a `gmq.Broker` instead of opening Redis inside business functions.
The generated `gmq.MockBroker` uses `github.com/golang/mock/gomock` v1.6.0.
Use the same package; mocks from another gomock module are not interchangeable.

The runnable [example test](../examples/testing/enqueue_test.go) verifies an application producer's
queue, stable ID, payload, uniqueness option, and error propagation without an external broker.
It covers success, duplicate IDs, and backend failures.

```go
controller := gomock.NewController(t)
broker := gmq.NewMockBroker(controller)
ctx := context.Background()
msg := &gmq.Msg{Id: "report-2026-10-19", Queue: "reports"}
broker.EXPECT().Enqueue(ctx, msg).Return(msg, nil)

client, err := gmq.NewClientFromBroker(broker)
if err != nil {
    t.Fatal(err)
}
if _, err := client.Enqueue(ctx, msg); err != nil {
    t.Fatal(err)
}
```

`NewController(t)` registers cleanup automatically.
Use exact arguments or `DoAndReturn` to check fields and variadic options.
Avoid `AnyTimes` for required effects; compare wrapped errors with `errors.Is`.
Test handlers directly with `ProcessMsg` and injected dependencies instead of starting background servers.
Mocks verify interactions, not storage guarantees; retain backend tests for atomicity and persistence.

## Commands

```sh
go test ./examples/testing -count=1
go test ./gmq -run TestBrokerSQLite -count=1
go test -timeout=5m ./...
go vet ./...
```

`make test` and `make typecheck` wrap the last two commands.
Redis tests skip when `GMQ_RDS` is unset. Use a disposable Redis database:

```powershell
$env:GMQ_RDS = "redis://localhost:6379/14?dial_timeout=1s&read_timeout=1s&max_retries=1"
go test -timeout=5m ./...
```

```sh
export GMQ_RDS='redis://localhost:6379/14?dial_timeout=1s&read_timeout=1s&max_retries=1'
go test -timeout=5m ./...
```

Regenerate mocks after changing `Broker` with `make genMock`, or:

```sh
go run github.com/golang/mock/mockgen@v1.6.0 -source=gmq/broker.go -destination=gmq/broker_mock.go -package=gmq
```

Run `make format` before committing and `make spell` to check repository spelling.
