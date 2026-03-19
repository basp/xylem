# Xylem Guide

> A composable, diagnostic-first ETL library written in idiomatic F#.

---

## `Source<'T>`

A `Source<'T>` is the **entry point** of any Xylem pipeline. It produces a
stream of records of type `'T` as an `IAsyncEnumerable<'T>`.

### Type definition

```fsharp
type Source<'T> = {
    Read: unit -> IAsyncEnumerable<'T>
}
```

The `Read` field is a function so that a source can be re-executed —
calling `Read ()` starts a fresh stream each time. The source itself is
an immutable record; any state (e.g. a database cursor) lives *inside*
the closure returned by `Read`.

### Why `IAsyncEnumerable<'T>`?

- **Streaming** — records are produced and consumed one at a time; the
  entire dataset never needs to be in memory at once.
- **Backpressure** — the consumer drives the pace via `MoveNextAsync()`.
- **Cancellation** — `CancellationToken` flows through
  `GetAsyncEnumerator(ct)` naturally.
- **Unbounded sources** — databases, files, queues, and live event
  streams all model cleanly as async sequences.

### Creating a source

Use the `taskSeq { }` computation expression from
`FSharp.Control.TaskSeq` to produce values:

```fsharp
open FSharp.Control

let numbersSource : Source<int> = {
    Read = fun () ->
        taskSeq {
            yield 1
            yield 2
            yield 3
        }
}
```

### Consuming a source in tests

Use `TaskSeq.toListAsync` to materialize the stream into a plain list:

```fsharp
let items = numbersSource.Read() |> TaskSeq.toListAsync |> Async.AwaitTask |> Async.RunSynchronously
// items = [1; 2; 3]
```

Or with `task { }`:

```fsharp
let! items = numbersSource.Read() |> TaskSeq.toListAsync
// items = [1; 2; 3]
```
