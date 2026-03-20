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

// Group items into arrays of at most N
let inPairsOf3 : Flow<int, int[]> = Flow.batch 3
```

### Composing flows

Two flows can be composed into one with `Flow.compose` (or the `>>>` operator):

```fsharp
let doubledEvens : Flow<int, int> =
    Flow.map (fun x -> x * 2) >>> Flow.filter (fun x -> x % 2 = 0)
```

Composition is lazy — no work happens until the stream is consumed.

> **Operator scope:** `>>>` is defined inside `module Flow` and is
> available when that module is open. When calling from a context where
> the module is not open, use `Flow.compose` directly:
>
> ```fsharp
> let flow = Flow.compose (Flow.map (fun x -> x * 2)) (Flow.filter (fun x -> x % 2 = 0))
> ```

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

## `ExecutionContext`

An `ExecutionContext` coordinates a single pipeline run. It carries
everything a flow or combinator needs at runtime — without baking
run-specific concerns into the `Flow` type itself.

### Type definition

```fsharp
type ExecutionContext = {
    CancellationToken: System.Threading.CancellationToken
    BatchSize:         int
    Emit:              DiagnosticEvent -> unit
    ReadEvents:        unit -> DiagnosticEvent list
}
```

| Field | Purpose |
|---|---|
| `CancellationToken` | Signals cooperative cancellation; ctx-aware flows check it per item |
| `BatchSize` | Preferred number of records per batch for batch-aware sinks and flows |
| `Emit` | Records a `DiagnosticEvent` for the current run |
| `ReadEvents` | Returns all events emitted so far, in emission order |

### Creating a context

```fsharp
// Sensible defaults — CancellationToken.None, BatchSize 1 000
let ctx = ExecutionContext.``default`` ()

// Explicit token and batch size
use cts = new System.Threading.CancellationTokenSource()
let ctx = ExecutionContext.create cts.Token 500
```

`ExecutionContext.create` wires `Emit` and `ReadEvents` to the same
internal `ResizeArray`, so every event emitted during a run is
retrievable at the end.

> **Note:** The current `Emit` implementation is not thread-safe.
> Concurrent flow execution is a v2 concern; for now all ctx-aware
> combinators iterate sequentially.

---

## Context-aware flow combinators

Pure flows (`map`, `filter`, `compose`) have no side effects and need no
context. Combinators that can *reject or fail records* receive an
`ExecutionContext` at construction time so they can emit structured
diagnostics without changing the `Flow` type.

### `Flow.validate`

Validates every item; passes `Ok` items downstream unchanged and drops
`Error` items, emitting one `DiagnosticEvent` of severity `Error` per
rejection.

```fsharp
let flow : Flow<int, int> =
    Flow.validate "check-positive" ctx (fun x ->
        if x > 0 then Ok x
        else Result.Error (ValidationError("value", "must be positive")))
```

The validator signature is `'T -> Result<'T, ErrorKind>` — the output
type is the same as the input type. Use `enrich` when you need a type
change.

Each emitted event records:
- `Stage` — the `stageName` string passed at construction
- `RecordIndex` — the 0-based position of the rejected record in the stream
- `Kind` — exactly the `ErrorKind` returned by the validator

The combinator calls `CancellationToken.ThrowIfCancellationRequested()`
at the start of each iteration, so an already-cancelled token stops the
stream immediately.

### `Flow.enrich`

Enriches every item using a function that may change the record type.
`Ok` items are passed downstream as the enriched value; `Error` items
are dropped with an `Error` diagnostic — same pattern as `validate`.

```fsharp
// int -> string enrichment (type changes)
let flow : Flow<int, string> =
    Flow.enrich "add-label" ctx (fun x ->
        if x > 0 then Ok $"item-{x}"
        else Result.Error (ValidationError("value", "must be positive")))
```

The enricher signature is `'T -> Result<'TOut, ErrorKind>`, making
`enrich` the right tool for:

- **Lookups** — resolve an ID to a full record
- **Projections** — reshape a row into a different type
- **Joins** — attach related data from another source

> **`validate` vs `enrich`:**
> `validate` is a special case of `enrich` where `'T = 'TOut` — it keeps
> the shape, only the record's worthiness is in question. Use `validate`
> when you are checking; use `enrich` when you are transforming.

---

## `Flow.batch`

`Flow.batch` groups a stream of individual items into a stream of
fixed-size arrays. It is a pure structural transform — it needs no
`ExecutionContext` and emits no diagnostics.

```fsharp
// Groups items into arrays of at most 100
let flow : Flow<Row, Row[]> = Flow.batch 100
```

### Behaviour

| Scenario | Result |
|---|---|
| 9 items, batchSize 3 | Three arrays of `[3; 3; 3]` |
| 10 items, batchSize 3 | Three full + one partial: `[3; 3; 3; 1]` |
| 2 items, batchSize 100 | One array of `[2]` |
| 0 items | Empty stream — no arrays emitted |
| batchSize 1 | One array per item |
| batchSize < 1 | `ArgumentException` thrown immediately |

**The partial last batch is always emitted.** Records are never silently
dropped because the final group is smaller than `batchSize`.

### Using `ctx.BatchSize`

`BatchSize` is a first-class field on `ExecutionContext` for exactly
this purpose. Pass it directly to respect the pipeline's configured
batch size:

```fsharp
let flow = Flow.batch ctx.BatchSize
```

This keeps the batch size in one place — the context — rather than
hard-coding it at each call site.

### Output type: `'T[]`

`batch` emits `'T[]` (array), not `'T list` or `seq<'T>`. Arrays are:

- **Fixed-size** — the sink knows exactly how many records it received
- **Contiguous** — optimal for bulk-insert APIs (SQL `SqlBulkCopy`,
  `IDataReader`, etc.)
- **Efficient** — `ResizeArray` is used internally; `ToArray()` is a
  single allocation per batch

`'T list` would be equally safe but adds an allocation and a traversal
for callers that need to hand the batch to a .NET API expecting an array.

### Composing `batch` with other flows

Because the output type changes from `'T` to `'T[]`, `batch` is
typically the **last** flow in a composed pipeline:

```fsharp
// validate, then enrich, then group into batches for bulk insert
let flow =
    Flow.compose
        (Flow.validate "check" ctx validator)
        (Flow.compose
            (Flow.enrich "enrich" ctx enricher)
            (Flow.batch ctx.BatchSize))
```

A sink that processes `'T[]` directly handles the batch as a unit:

```fsharp
let bulkSink : Sink<Row[]> = {
    Write = fun batches -> task {
        for batch in batches do
            do! db.BulkInsertAsync(batch)
    }
}
```

---

## Running a pipeline with context

`Pipeline.runWithContext` is the full-featured runner. It wraps
`runWith`, counts every record emitted by the source, measures
wall-clock duration, and collects all diagnostic events from `ctx`:

```fsharp
let ctx = ExecutionContext.``default`` ()

let! result : PipelineResult = Pipeline.runWithContext ctx source flow sink
```

`result.RecordsRead`, `result.RecordsAccepted`, `result.RecordsRejected`,
and `result.RecordsFailed` are derived from the events in `ctx` — they
are always consistent with `result.Events`.

```fsharp
printfn $"Read: %d{result.RecordsRead}  Accepted: %d{result.RecordsAccepted}  Rejected: %d{result.RecordsRejected}"
for e in result.Events do
    printfn $"[%A{e.Severity}] stage=%A{e.Stage} index=%A{e.RecordIndex} — %s{e.Message}"
```

The runner checks `ctx.CancellationToken` before starting so that an
already-cancelled token throws immediately without touching the source.

---

## Connectors

A **connector** is a `Source<'T>` or `Sink<'T>` that ties the pipeline
to a specific data store or transport. The core library ships one
connector out of the box: `Xylem.Connectors.InMemory`. File, JSON/CSV,
database, and queue connectors are planned for future releases.

### `Xylem.Connectors.InMemory`

The in-memory connector requires no I/O and no external dependencies.
It is the go-to choice for tests, examples, and simple one-off
pipelines.

```fsharp
open Xylem.Connectors
```

#### `InMemory.source`

Creates a `Source<'T>` from any `seq<'T>`-compatible value — lists,
arrays, and sequences all work:

```fsharp
let source = InMemory.source [1; 2; 3; 4; 5]
let source = InMemory.source [| "a"; "b"; "c" |]
let source = InMemory.source (seq { for i in 1..100 do yield i })
```

Each call to `source.Read()` produces a fresh, independent
`IAsyncEnumerable<'T>` — the source behaves exactly like any other
Xylem source. Multiple runs over the same source are safe as long as
the underlying sequence is re-iterable (lists and arrays always are;
one-shot `seq` expressions are not).

#### `InMemory.sink`

Creates a `Sink<'T>` that accumulates every written item into an
internal buffer. Returns the sink together with a **reader function**
that snapshots the collected items on demand:

```fsharp
let sink, read = InMemory.sink ()

do! Pipeline.runWith source flow sink

let items : int list = read ()   // ["item-2"; "item-4"; ...]
```

Each call to `read ()` returns a fresh `'T list` — the same items, a
different list object. This mirrors the `ExecutionContext.ReadEvents`
pattern and makes call-site assertions straightforward:

```fsharp
Assert.Equal<int list>([2; 4], read ())
```

#### Full pipeline example

```fsharp
open Xylem.Connectors

let ctx    = ExecutionContext.``default`` ()
let source = InMemory.source [1; -2; 3; -4; 5]
let flow   = Flow.validate "check-positive" ctx (fun x ->
    if x > 0 then Ok x
    else Result.Error (ValidationError("value", "must be positive")))
let sink, read = InMemory.sink ()

let! result = Pipeline.runWithContext ctx source flow sink

printfn $"Accepted: %A{read ()}"          // [1; 3; 5]
printfn $"Rejected: %d{result.RecordsRejected}"  // 2
```

---

### Design decisions — `InMemory`

#### `#seq<'T>` vs `seq<'T>` for the source input

`InMemory.source` accepts `#seq<'T>` (a flexible type) rather than
`seq<'T>`. The difference: with `seq<'T>`, passing a `list` or
`array` boxes it into a plain `seq` before the function is entered —
the static type is lost. With `#seq<'T>`, the compiler accepts any
subtype of `IEnumerable<'T>` without an upcast, keeping the most
specific type at the call site.

In practice the difference is invisible at runtime, but `#seq<'T>` is
the idiomatic F# choice for functions that accept *any* sequence.

#### `unit -> 'T list` reader vs returning the list directly

`InMemory.sink` returns `Sink<'T> * (unit -> 'T list)` rather than
`Sink<'T> * 'T list`. If it returned the list directly, the list would
be captured at construction time — before any items have been written —
and would always be empty.

The reader function defers evaluation: each call snapshots the buffer
*at that moment*, which is what tests and post-run inspection actually
need. This is the same deferred-reader pattern used by
`ExecutionContext.ReadEvents`.

#### `'T list` vs `ResizeArray<'T>` in the snapshot

The reader converts the internal `ResizeArray<'T>` to a `'T list` on
every call via `List.ofSeq`. This means:

- **Immutable** — callers can hold onto a snapshot without worrying
  about it changing under them.
- **Idiomatic** — F# assertion helpers and pattern matching work
  naturally on lists.
- **One allocation per read** — acceptable for the test/example use
  case this connector targets; not a concern for production connectors
  where the sink itself owns the write strategy.

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

- **Type explosion on composition.**<br/>After chaining two flows the return
  type becomes
  `IAsyncEnumerable<Result<Result<'C, DiagnosticEvent>, DiagnosticEvent>>`.
  A `bind`-style compose flattens it, but the ergonomics deteriorate
  quickly and the engine must understand the nesting.
- **Warnings are unrepresentable.**<br/>A record that *passes* validation but
  triggers a warning (e.g. a coerced null) must be `Ok` — there is no
  channel for "healthy record, but here is a note". You would need
  `Result<'TOut * DiagnosticEvent list, DiagnosticEvent list>`, which is
  a very complex return type.
- **Most flows don't reject anything.**<br/>`map` and `filter` are pure
  transforms. Forcing all flows to wrap their output in `Result` for the
  sake of a few validation flows is a poor trade.

### Option B — `ExecutionContext` with `Emit` (chosen, implemented)

A context object is threaded through diagnostics-aware combinators:

```fsharp
type ExecutionContext = {
    CancellationToken: System.Threading.CancellationToken
    BatchSize:         int
    Emit:              DiagnosticEvent -> unit
    ReadEvents:        unit -> DiagnosticEvent list
}
```

Flows that need to emit events receive a context at *construction time*,
not as part of the `Flow` type itself:

```fsharp
// Pure flow — no context needed, clean signature
let doubled = Flow.map (fun x -> x * 2)

// Validating flow — opts into context at construction time
let validateAge ctx =
    Flow.validate "check-age" ctx (fun person ->
        if person.Age < 0 then
            Result.Error (ValidationError("Age", "must be >= 0"))
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

`ExecutionContext` and the ctx-aware combinators (`validate`, `enrich`)
are now implemented. Pure flows (`map`, `filter`, `compose`) remain
completely side-effect free and need no context.
