<!-- cspell:ignore Asynq -->

# Features and comparison

gmq separates producers (`Client`), storage (`Broker`), and consumers (`Server` and `Mux`).
Handlers receive a context and an `IMsg`; returning an error records a failure.

- Concurrent workers per queue, defaulting to `runtime.NumCPU()`.
- Configurable per-worker consume intervals, including dynamic intervals.
- Message ID deduplication for a configurable duration.
- Redis, SQLite, and in-memory storage brokers.
- Pause/resume, failure history, queue statistics, and background cleanup.
- Recurring enqueueing through a cron scheduler.
- Handler middleware and a Redis management CLI.
- Generated gomock broker for isolated application tests.

## Choosing gmq

The interface was inspired by [Asynq](https://github.com/hibiken/asynq).
gmq emphasizes practical pacing and replaceable brokers. Consume intervals control how frequently
workers process messages, independently of scheduling a future job or delaying a retry.
Its ID reservation remains effective within `OptUniqueIn` after success or failure.
Use a stable explicit ID to deduplicate business operations.

| Requirement               | gmq approach                 | Consideration                                              |
| ------------------------- | ---------------------------- | ---------------------------------------------------------- |
| Pace external requests    | Per-worker interval callback | Multiple processes have independent pacing                 |
| Local persistent queue    | SQLite without CGO or Redis  | SQLite serializes writes; use a local file                 |
| Shared queue across hosts | Redis broker                 | Configure Redis persistence and availability separately    |
| Process-local queue       | In-memory broker             | Data disappears on close or process exit                   |
| Automatic retries         | Failures are archived        | Implement an explicit retry workflow                       |
| Durable event stream      | Work queue API               | Evaluate a streaming system for consumer groups and replay |

Usability takes priority over maximum throughput or fault-tolerance machinery.
Do not assume automatic recovery of abandoned claims, exactly-once execution, or a distributed rate limit.
Make externally visible handler effects idempotent.
The wiki's detailed Asynq option comparison describes historical behavior;
consult that project's current documentation when comparing versions.
