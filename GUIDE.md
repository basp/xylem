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

---

## Diagnostics model

Xylem's diagnostics are designed around one principle: **failures must be
loud, structured, and traceable.** A string error message is not enough —
you need to know *what* failed, *why*, *where* in the pipeline, and *which
record* was involved.

### `Severity`

Every diagnostic event carries a severity level:

```fsharp
type Severity =
    | Info      // Something noteworthy; pipeline is healthy
    | Warning   // Unexpected but recoverable; pipeline continues
    | Error     // A record could not be processed; it is rejected
    | Fatal     // Pipeline cannot continue; execution is aborted
```

### `ErrorKind`

The `ErrorKind` discriminated union describes *what went wrong*. The
well-known cases are machine-readable and pattern-matchable; `Custom` is
the extension point for domain-specific categories:

```fsharp
type ErrorKind =
    | SystemError           of exn
    | IoError               of path: string * exn
    | ValidationError       of field: string * reason: string
    | BusinessRuleViolation of rule: string * reason: string
    | PipelineError         of stage: string * exn
    | Custom                of tag: string * data: Map<string, string>
```

**Adding a new well-known case is intentionally a breaking change.**
Exhaustive pattern matches will fail to compile, forcing every caller to
explicitly handle the new category. `Custom` is the safety valve when
you need a domain-specific kind without modifying the library.

### `DiagnosticEvent`

A single structured event emitted during a pipeline run:

```fsharp
type DiagnosticEvent = {
    Severity:    Severity
    Kind:        ErrorKind
    Stage:       string option        // which flow/stage emitted this
    RecordIndex: int64 option         // 0-based record position, if applicable
    Timestamp:   DateTimeOffset
    Message:     string               // human-readable summary
}
```

`Stage` and `RecordIndex` are both `option` because not every event is
tied to a specific stage or record (e.g. a file-open failure has no
record index; a pre-flight config check has no stage).

### `PipelineResult`

The structured outcome of a completed pipeline run:

```fsharp
type PipelineResult = {
    RecordsRead:     int64
    RecordsAccepted: int64
    RecordsRejected: int64
    RecordsFailed:   int64
    Duration:        TimeSpan
    Events:          DiagnosticEvent list
}
```

`Events` is the source of truth. The counts are pre-computed
conveniences — they are always consistent with `Events` and save callers
from folding the list themselves.

---

## Design decision: how flows emit diagnostics

This decision is worth documenting in full because the alternatives have
non-obvious trade-offs.

### Option A — `Result` in the stream (rejected)

The most obviously functional approach: flows return
`IAsyncEnumerable<Result<'TOut, DiagnosticEvent>>` so rejections are
inline:

```fsharp
type Flow<'TIn, 'TOut> = {
    Transform: IAsyncEnumerable<'TIn> -> IAsyncEnumerable<Result<'TOut, DiagnosticEvent>>
}
```

**Why we didn't choose this:**

- **Type explosion on composition.** After chaining two flows the return
  type becomes
  `IAsyncEnumerable<Result<Result<'C, DiagnosticEvent>, DiagnosticEvent>>`.
  A `bind`-style compose flattens it, but the ergonomics deteriorate
  quickly and the engine must understand the nesting.
- **Warnings are unrepresentable.** A record that *passes* validation but
  triggers a warning (e.g. a coerced null) must be `Ok` — there is no
  channel for "healthy record, but here is a note". You would need
  `Result<'TOut * DiagnosticEvent list, DiagnosticEvent list>`, which is
  a very complex return type.
- **Most flows don't reject anything.** `map` and `filter` are pure
  transforms. Forcing all flows to wrap their output in `Result` for the
  sake of a few validation flows is a poor trade.

### Option B — `ExecutionContext` with `Emit` (chosen, deferred)

A context object is threaded through diagnostics-aware combinators:

```fsharp
type ExecutionContext = {
    Emit: DiagnosticEvent -> unit
    // future: CancellationToken, BatchSize, ...
}
```

Flows that need to emit events receive a context at *construction time*,
not as part of the `Flow` type itself:

```fsharp
// Pure flow — no context needed, clean signature
let doubled = Flow.map (fun x -> x * 2)

// Validating flow — opts into context at construction time
let validateAge ctx =
    Flow.validate ctx (fun person ->
        if person.Age < 0 then
            Error (ValidationError("Age", "must be >= 0"))
        else
            Ok person)
```

**Why this works:**

- `Flow<'TIn,'TOut>` stays exactly as it is. No type changes, no
  breaking changes to existing combinators.
- Any event at any time: warnings on healthy records, multiple errors per
  record, informational events mid-stream — all natural.
- Pure flows (`map`, `filter`, `compose`) remain completely side-effect
  free and need no context.
- Only flows that *opt in* to diagnostics touch `ctx`.

**The drawback:** `ctx.Emit` is a side effect. Flows that use it are no
longer purely functional — they produce output *and* write to the context.
This is a deliberate pragmatic choice. Real ETL pipelines inherently
produce side effects (writing files, hitting databases); pretending
diagnostics can be fully pure adds complexity without benefit.

**Deferred:** `ExecutionContext` and `ctx`-aware combinators (`validate`,
`enrich`, etc.) are introduced in the next increment. The diagnostics
*types* (`Severity`, `ErrorKind`, `DiagnosticEvent`, `PipelineResult`)
are defined now so tests and flows can reference them immediately.
