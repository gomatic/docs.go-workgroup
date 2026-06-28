---
title: go-workgroup
---

`go-workgroup` is the gomatic ecosystem's **concurrent worker-group** primitive for Go. It distributes work from a single producer across N consumer goroutines with type-safe generics, structured [`slog`](https://pkg.go.dev/log/slog) logging, and configurable error handling. A [`Source[T]`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Source) feeds items into an internal channel, a pool of [`Worker[T]`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Worker) goroutines consumes them, and [`Run`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Run) blocks until the work is done or the context is cancelled — returning the first error (fail-fast) or every error joined ([`CollectAll`](https://pkg.go.dev/github.com/gomatic/go-workgroup#CollectAll)).

- **Source:** [`gomatic/go-workgroup`](https://github.com/gomatic/go-workgroup)
- **API reference:** [pkg.go.dev/github.com/gomatic/go-workgroup](https://pkg.go.dev/github.com/gomatic/go-workgroup)

## Install

```sh
go get github.com/gomatic/go-workgroup
```

Requires Go 1.26+.

## Why a worker group

Spinning up goroutines by hand for a producer/consumer workload means re-deriving the same plumbing every time: a work channel, a `sync.WaitGroup` per stage, context cancellation propagated to every worker, and a thread-safe place to collect errors. Getting any one of these wrong leaks goroutines or deadlocks on a blocked send. `go-workgroup` owns that plumbing once. You supply only the two functions that are actually domain-specific — a [`Source[T]`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Source) that produces items and a [`Worker[T]`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Worker) that consumes one — and the package runs them concurrently, cancels cleanly, and aggregates the result.

The two function types are the whole contract:

```go
import workgroup "github.com/gomatic/go-workgroup"

// A Source sends work items to the channel until it is done or ctx is cancelled.
type Source[T any] func(context.Context, chan<- T) error

// A Worker processes one item; id is the 0-based worker index.
type Worker[T any] func(context.Context, int, T) error
```

## Usage

### Fan-out: distribute work across workers

[`FanOut`](https://pkg.go.dev/github.com/gomatic/go-workgroup#FanOut) runs the source against `n` concurrent workers:

```go
package main

import (
	"context"
	"fmt"

	workgroup "github.com/gomatic/go-workgroup"
)

func main() {
	source := workgroup.Source[int](func(ctx context.Context, out chan<- int) error {
		for i := range 100 {
			select {
			case out <- i:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	worker := workgroup.Worker[int](func(ctx context.Context, id int, item int) error {
		fmt.Printf("worker %d processed item %d\n", id, item)
		return nil
	})

	if err := workgroup.FanOut(context.Background(), 8, source, worker); err != nil {
		panic(err)
	}
}
```

### Fan-in: a single worker

[`FanIn`](https://pkg.go.dev/github.com/gomatic/go-workgroup#FanIn) is [`Run`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Run) with exactly one worker — every item is processed serially by one goroutine, useful for aggregating a fan-out stage's output into shared state without a mutex:

```go
err := workgroup.FanIn(ctx, source, worker)
```

### Run: explicit options

[`Run`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Run) is the underlying entry point; [`FanOut`](https://pkg.go.dev/github.com/gomatic/go-workgroup#FanOut) and [`FanIn`](https://pkg.go.dev/github.com/gomatic/go-workgroup#FanIn) are thin wrappers that preset [`Workers`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Workers). Pass any combination of options:

```go
err := workgroup.Run(ctx, source, worker,
	workgroup.Workers(16),
	workgroup.Name("processor"),
	workgroup.Log{Logger: slog.Default()},
	workgroup.CollectAll,
)
```

With no options, [`Run`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Run) defaults to [`runtime.NumCPU()`](https://pkg.go.dev/runtime#NumCPU) workers, [`slog.Default()`](https://pkg.go.dev/log/slog#Default), and fail-fast error handling.

### Pipe: chain stages

[`Pipe`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Pipe) turns a [`Transformer[In, Out]`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Transformer) into a new [`Source[Out]`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Source), so a fan-out transform feeds a downstream stage. The returned source runs the upstream source with `n` workers applying the transform, emitting results for the next [`Run`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Run):

```go
doubled := workgroup.Pipe(4, source,
	func(ctx context.Context, id int, item int) (int, error) {
		return item * 2, nil
	},
)

err := workgroup.FanIn(ctx, doubled, aggregator)
```

### Error handling

The default is **fail-fast**: the first worker error cancels the shared context, every other worker stops, and that error is returned.

```go
// Fail-fast (default): the first error cancels all workers.
err := workgroup.Run(ctx, source, riskyWorker)
```

Pass [`CollectAll`](https://pkg.go.dev/github.com/gomatic/go-workgroup#CollectAll) to keep processing every item and aggregate the failures — the returned error joins them with [`errors.Join`](https://pkg.go.dev/errors#Join), so each is recoverable with [`errors.Is`](https://pkg.go.dev/errors#Is):

```go
// Collect-all: continue processing, join all errors at the end.
err := workgroup.Run(ctx, source, riskyWorker, workgroup.CollectAll)
// err contains every worker error via errors.Join.
```

A `nil` source or `nil` worker is rejected before any goroutine starts, with the sentinels [`ErrNilSource`](https://pkg.go.dev/github.com/gomatic/go-workgroup#pkg-constants) and [`ErrNilWorker`](https://pkg.go.dev/github.com/gomatic/go-workgroup#pkg-constants), both matchable with [`errors.Is`](https://pkg.go.dev/errors#Is).

## API summary

| Function | Description |
|----------|-------------|
| [`Run[T]`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Run) | Distribute work from a source across N workers (default: `NumCPU`). |
| [`FanOut[T]`](https://pkg.go.dev/github.com/gomatic/go-workgroup#FanOut) | `Run` with `Workers(n)`. |
| [`FanIn[T]`](https://pkg.go.dev/github.com/gomatic/go-workgroup#FanIn) | `Run` with `Workers(1)`. |
| [`Pipe[In, Out]`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Pipe) | Build a `Source` from a transformation for stage chaining. |

| Option | Default | Description |
|--------|---------|-------------|
| [`Workers`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Workers) | `runtime.NumCPU()` | Number of concurrent worker goroutines. |
| [`Name`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Name) | `""` | Workgroup name included in log output. |
| [`Log`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Log) | `slog.Default()` | Structured logger. |
| [`FailFast`](https://pkg.go.dev/github.com/gomatic/go-workgroup#FailFast) | yes | Cancel all workers on the first error. |
| [`CollectAll`](https://pkg.go.dev/github.com/gomatic/go-workgroup#CollectAll) | no | Continue, join all errors at the end. |

## Design

- **Generic, value-typed contract.** [`Source[T]`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Source), [`Worker[T]`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Worker), and [`Transformer[In, Out]`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Transformer) are plain function types — no interfaces to implement, no work-item boxing into `any`.
- **Options are typed values.** Each option ([`Workers`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Workers), [`Name`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Name), [`Log`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Log), [`FailFast`](https://pkg.go.dev/github.com/gomatic/go-workgroup#FailFast)/[`CollectAll`](https://pkg.go.dev/github.com/gomatic/go-workgroup#CollectAll)) is a small named type satisfying the [`Optional`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Optional) interface, so configuration is self-describing and `nil` options are skipped.
- **Clean cancellation.** The group runs under a [`context.WithCancelCause`](https://pkg.go.dev/context#WithCancelCause); a source or worker failure cancels it with the causing error. After every worker returns, the package drains any item still pending on the channel, so even a non-cooperative source that ignores `ctx` and blocks on a raw send is unblocked rather than deadlocking the group.
- **Deterministic outcome.** The final error is resolved in priority order: worker errors first, then a real (non-context) source error, then any context cancellation — otherwise the run completed successfully and `nil` is returned.
- **Structured logging built in.** Start, success, and error transitions are logged via [`slog.LogAttrs`](https://pkg.go.dev/log/slog#Logger.LogAttrs) with the worker count and optional name as attributes.
- **Sentinel errors.** The package's own failures are constants of an [`Error`](https://pkg.go.dev/github.com/gomatic/go-workgroup#Error) string newtype whose `.With(cause, args...)` wraps with `%w`, mirroring the [`gomatic/go-error`](https://github.com/gomatic/go-error) mechanism so error construction stays uniform across the ecosystem.
- **Dependency-light** — the package depends only on the standard library (`context`, `errors`, `log/slog`, `runtime`, `sync`).

## Who uses it

`go-workgroup` is the shared concurrency primitive for gomatic Go projects that fan work across goroutines, alongside the rest of the [`gomatic/go-*`](https://github.com/orgs/gomatic/repositories?q=go-) libraries.
