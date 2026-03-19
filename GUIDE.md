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

---

## `Sink<'T>`

A `Sink<'T>` is the **exit point** of a pipeline. It consumes an
`IAsyncEnumerable<'T>` stream and returns `Task<unit>` once all records
have been processed.

### Type definition

```fsharp
type Sink<'T> = {
    Write: IAsyncEnumerable<'T> -> Task<unit>
}
```

### Why pass the whole stream?

Giving the sink the entire `IAsyncEnumerable<'T>` lets it control its
own iteration strategy:

- A **collecting sink** iterates one item at a time.
- A **database sink** can buffer records into batches before committing.
- A **file sink** can open and close the resource around the whole
  stream rather than per-item.

An item-by-item `Write: 'T -> Task<unit>` interface would force the
pipeline engine to drive iteration, removing that flexibility.

### Creating a sink

The simplest sink is an in-memory collector — useful in tests:

```fsharp
open System.Collections.Generic
open FSharp.Control

let collectSink () =
    let collected = List<'T>()
    let sink : Sink<'T> = {
        Write = fun stream -> task {
            do! stream |> TaskSeq.iter (fun item -> collected.Add(item))
        }
    }
    sink, collected
```

### Connecting a source to a sink

Use `Pipeline.run` to wire a `Source` to a `Sink`:

```fsharp
do! Pipeline.run source sink
```

`Pipeline.run` simply passes the source stream to the sink's `Write`
function:

```fsharp
let run (source: Source<'T>) (sink: Sink<'T>) : Task<unit> =
    sink.Write(source.Read())
```

---

## `Flow<'TIn,'TOut>`

A `Flow<'TIn,'TOut>` sits **between** a `Source` and a `Sink`. It
transforms an `IAsyncEnumerable<'TIn>` into an
`IAsyncEnumerable<'TOut>` — lazily, without materializing the stream.

### Type definition

```fsharp
type Flow<'TIn, 'TOut> = {
    Transform: IAsyncEnumerable<'TIn> -> IAsyncEnumerable<'TOut>
}
```

### Why a stream-to-stream function?

Passing the whole stream (rather than item-by-item) gives the flow full
control over its iteration strategy:

- A **map** flow transforms each item individually.
- A **filter** flow skips items that don't match a predicate.
- A **batch** flow (future) can group items into chunks before emitting.
- A **window** flow (future) can look ahead or behind.

All of these are impossible with an item-by-item `'TIn -> 'TOut`
signature.

### Built-in flow combinators

```fsharp
// Transform every item
let doubled : Flow<int, int> = Flow.map (fun x -> x * 2)

// Keep only matching items
let evens : Flow<int, int> = Flow.filter (fun x -> x % 2 = 0)
```

### Composing flows

Two flows can be composed into one with `Flow.compose` (or the `>>>` operator):

```fsharp
let doubledEvens : Flow<int, int> =
    Flow.map (fun x -> x * 2) >>> Flow.filter (fun x -> x % 2 = 0)
```

Composition is lazy — no work happens until the stream is consumed.

### Connecting source, flow, and sink

Use `Pipeline.runWith` to wire all three together:

```fsharp
do! Pipeline.runWith source flow sink
```

`Pipeline.runWith` threads the stream through the flow before handing
it to the sink:

```fsharp
let runWith (source: Source<'TIn>) (flow: Flow<'TIn,'TOut>) (sink: Sink<'TOut>) : Task<unit> =
    sink.Write(flow.Transform(source.Read()))
```
