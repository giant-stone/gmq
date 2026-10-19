# Built-in and custom brokers

Clients, servers, and schedulers accept the [Broker interface](../gmq/broker.go).

| Broker    | Constructor                      | Scope                                           |
| --------- | -------------------------------- | ----------------------------------------------- |
| Redis     | `NewBrokerRedis(dsn, namespace)` | Shared instance; namespace separates key spaces |
| SQLite    | `NewBrokerSQLite(filename)`      | Persistent local file without CGO               |
| In-memory | `NewBrokerInMemory(opts)`        | Process-local; discarded on close               |

Pass the broker to `NewClientFromBroker` and `NewServer`.
`Client.Close` and `Server.Shutdown` close their broker; coordinate ownership when sharing it.
Stop producers and schedulers before shutting down a server that shares their broker.

## SQLite

`NewBrokerSQLite` accepts a filename, SQLite `file:` URI, or `:memory:` for an isolated temporary database.
Create the parent directory first. Embedded migrations run in a transaction;
models, schemas, and migrations live under [schemas](../schemas).
File databases use WAL mode. Instances can share a local file with atomic claims;
write contention waits up to five seconds. SQLite permits one writer at a time.
Processing messages remain processing after reopening; recovery requires explicit deletion and enqueueing.
The management CLI supports Redis, not SQLite.

## Implement a broker

Use the current `Broker` interface instead of copying the older wiki interface.
Implement atomic enqueue and claim operations, queue-scoped ID reservation, completion, failure recording,
initialization, cleanup, pause/resume, inspection, statistics, clock injection, timezone selection, and close.
`Dequeue` should return `ErrNoMsg` when work cannot be claimed, including a paused queue.
Honor context cancellation and concurrent callers.

Start from [BrokerInMemory](../gmq/broker_memory.go) or [BrokerSQLite](../gmq/broker_sqlite.go).
Add a compile-time assertion for your implementation:

```go
var _ gmq.Broker = (*CustomBroker)(nil)
```

Then inject it normally:

```go
client, err := gmq.NewClientFromBroker(customBroker)
if err != nil {
    return err
}
server := gmq.NewServer(ctx, customBroker, config)
```

For a custom broker that supports a subset of operations, embed `BrokerUnimplemented` by value:

```go
type CustomBroker struct {
    gmq.BrokerUnimplemented
}

// Override the operations your application uses.
```

The helper satisfies `Broker`. Unsupported operations return errors wrapping
`ErrNotImplemented` with the method name; check them using `errors.Is`.
`Close` returns `nil`, while `SetClock` and `UTC` do nothing. Override these defaults
when your broker owns resources or supports time configuration. Embedding does not
provide storage behavior or guarantee compatibility with future interface changes.
Implement every operation reached by your application's runtime path.
Workers call `Init`, `Dequeue`, `Complete`, and `Fail`; the cleaner calls `DeleteAgo`;
server controls call `Pause`, `Resume`, and `Close`. Management tools need the corresponding inspection methods.

Test FIFO claims, concurrent claims without duplicates, uniqueness after success and failure,
pause/resume, cancellation, cleanup, failure history, statistics, and persistence where supported.
Use a simulated clock for deterministic expiration tests and backend tests for atomicity and persistence.
