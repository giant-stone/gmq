# gmq

gmq is a Go message queue with configurable consume intervals and replaceable storage brokers.

[![Go](https://github.com/giant-stone/gmq/actions/workflows/go.yml/badge.svg)](https://github.com/giant-stone/gmq/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/giant-stone/gmq)](https://goreportcard.com/report/github.com/giant-stone/gmq)

## Features

- Per-queue concurrency and custom consume intervals.
- Message ID deduplication that survives success or failure within its configured window.
- Redis, SQLite, and in-memory brokers behind a shared interface.
- Failure history, pause/resume, statistics, and automatic cleanup.
- Cron scheduling, handler middleware, and a Redis management CLI.
- Generated gomock broker for application unit tests.

## Getting started

Use a supported stable Go release; the module declares Go 1.20.

```sh
go get github.com/giant-stone/gmq@latest
```

Follow the [complete quickstart](docs/getting-started.md) for a producer and consumer using SQLite without external services.

## Documentation

See the [documentation index](docs/README.md) for features and comparisons, lifecycle,
custom consume rates, custom brokers, operations, and testing with gomock.
These guides update the [original wiki](https://github.com/giant-stone/gmq/wiki) for the current API.
Runnable applications live under [examples](examples).

## Development

```sh
make test
make typecheck
make format
make spell
```

The equivalent test and analysis commands are `go test -timeout=5m ./...` and `go vet ./...`.
Redis integration tests require `GMQ_RDS`; otherwise they skip.
See [testing](docs/testing.md) for connection configuration, mock generation, and focused commands.
