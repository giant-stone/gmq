# API cleanup before v1

These changes break source compatibility with earlier v0 releases.

- `IHandler` requires only `ProcessMsg(context.Context, IMsg) error`. Inject clocks and ID generators through your own handler constructor or fields.
- `HandlerFunc` and `Mux` no longer expose `SetClock` or `SetIdGenerator`.
- The unused `Handler` base struct and `IdGenerator` interface have been removed. Implement `ProcessMsg` directly instead of embedding `Handler`.
- `BrokerUnimplemented` now satisfies `Broker`, with corrected signatures and all missing methods. Embed it by value to implement a subset of operations. Unsupported operations wrap `ErrNotImplemented`; check with `errors.Is`. `Close` now returns `nil`; `SetClock` and `UTC` are no-ops. Built-in brokers implement every method explicitly.
- `Mux.GetPatterns()` returns a sorted `[]string` snapshot instead of a map of opaque entries. Iterate with `for _, pattern := range mux.GetPatterns()`. Changing the returned slice does not change registrations. The internal entry type is no longer exported.

Message processing, middleware, and the `Broker` interface signatures are otherwise unchanged.
