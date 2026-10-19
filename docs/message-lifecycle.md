# Message lifecycle and deduplication

```text
Enqueue -> pending -> Dequeue -> processing -> Complete -> removed from active work
                                           -> Fail     -> failed history
```

Public state constants include `pending`, `processing`, `failed`, and `completed`.
Completion does not promise continued payload availability: built-in brokers remove completed work
while preserving its uniqueness reservation. There is no stored `waiting` state for consume intervals.

A handler returning `nil` invokes `Complete`; an error invokes `Fail`.
Failures are recorded rather than automatically retried. Inspect them using `GetMsg`, `ListFailed`, and statistics.
A crash can leave messages processing; there is no automatic lease recovery.
Plan explicit recovery and make handlers idempotent.

## Deduplicate by stable ID

```go
const uniquenessWindow = 12 * time.Hour

_, err := client.Enqueue(ctx, &gmq.Msg{
    Id: "report-2026-10-19",
    Queue: "reports",
    Payload: []byte(`{"date":"2026-10-19"}`),
}, gmq.OptUniqueIn(uniquenessWindow))
if errors.Is(err, gmq.ErrMsgIdConflict) {
    // This ID is already reserved in this queue.
}
```

Use the same ID and queue for the same logical operation. Empty IDs are generated automatically;
identical payloads with empty IDs are not deduplicated by payload.
The default window is `gmq.DefaultTTLMsgUniq` (one day).
Success and failure do not release the reservation before its deadline.
Explicit `DeleteMsg` removes the message and reservation, permitting intentional enqueueing again.
This prevents duplicate enqueueing within the window, not duplicate external effects.

## Retention

`Created`, `Updated`, and `ExpiresAt` use Unix milliseconds.
Expiration and cleanup details are broker-specific; uniqueness is not an execution timeout.
The server's background cleaner uses `Config.MsgMaxTTL`, defaulting to seven days.
The library also defines `TTLDeadMsg` as three days for failure retention.
These constants do not guarantee identical payload retention across brokers;
check the selected broker's cleanup behavior and back up persistent storage separately.
