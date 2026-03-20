# Xylem Guide

> A composable, diagnostic-first ETL library written in idiomatic F#.

---

## `Root<'T>`

A `Root<'T>` is the **entry point** of any Xylem pipeline. It produces a
stream of records of type `'T` as an `IAsyncEnumerable<'T>`.

### Type definition

```fsharp
type Root<'T> = {
    Read: unit -> IAsyncEnumerable<'T>
}
```

The `Read` field is a function so that a root can be re-executed —
calling `Read ()` starts a fresh stream each time. The root itself is
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

### Creating a root

Use the `taskSeq { }` computation expression from
`FSharp.Control.TaskSeq` to produce values:

```fsharp
open FSharp.Control

let numbersRoot : Root<int> = {
    Read = fun () ->
        taskSeq {
            yield 1
            yield 2
            yield 3
        }
}
```

### Consuming a root in tests

Use `TaskSeq.toListAsync` to materialize the stream into a plain list:

```fsharp
let items = numbersRoot.Read() |> TaskSeq.toListAsync |> Async.AwaitTask |> Async.RunSynchronously
// items = [1; 2; 3]
```

Or with `task { }`:

```fsharp
let! items = numbersRoot.Read() |> TaskSeq.toListAsync
// items = [1; 2; 3]
```

---

## `Leaf<'T>`

A `Leaf<'T>` is the **exit point** of a pipeline. It consumes an
`IAsyncEnumerable<'T>` stream and returns `Task<unit>` once all records
have been processed.

### Type definition

```fsharp
type Leaf<'T> = {
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

### Creating a leaf

The simplest sink is an in-memory collector — useful in tests:

```fsharp
open System.Collections.Generic
open FSharp.Control

let collectSink () =
    let collected = List<'T>()
    let leaf : Leaf<'T> = {
        Write = fun stream -> task {
            do! stream |> TaskSeq.iter (fun item -> collected.Add(item))
        }
    }
    leaf, collected
```

### Connecting a root to a leaf

Use `Pipeline.run` to wire a `Root` to a `Leaf`:

```fsharp
do! Pipeline.run root leaf
```

`Pipeline.run` simply passes the root stream to the leaf's `Write`
function:

```fsharp
let run (root: Root<'T>) (leaf: Leaf<'T>) : Task<unit> =
    leaf.Write(root.Read())
```

---

## `Vessel<'TIn,'TOut>`

A `Vessel<'TIn,'TOut>` sits **between** a `Root` and a `Leaf`. It
transforms an `IAsyncEnumerable<'TIn>` into an
`IAsyncEnumerable<'TOut>` — lazily, without materializing the stream.

### Type definition

```fsharp
type Vessel<'TIn, 'TOut> = {
    Transform: IAsyncEnumerable<'TIn> -> IAsyncEnumerable<'TOut>
}
```

> **Vessel vs Pipeline:** A `Vessel` defines *what* transformation to apply
> — it is a reusable, composable value. The `Pipeline` module defines
> *how* to execute a complete `Root` → `Vessel` → `Leaf` chain. Think of a
> `Vessel` as a recipe and `Pipeline.runWithContext` as the kitchen that
> runs it.

### Why a stream-to-stream function?

Passing the whole stream (rather than item-by-item) gives the vessel full
control over its iteration strategy:

- A **map** vessel transforms each item individually.
- A **filter** vessel skips items that don't match a predicate.
- A **batch** vessel can group items into chunks before emitting.
- A **window** vessel (future) can look ahead or behind.

All of these are impossible with an item-by-item `'TIn -> 'TOut`
signature.

### Built-in vessel combinators

```fsharp
// Transform every item
let doubled : Vessel<int, int> = Vessel.map (fun x -> x * 2)

// Keep only matching items
let evens : Vessel<int, int> = Vessel.filter (fun x -> x % 2 = 0)

// Group items into arrays of at most N
let inPairsOf3 : Vessel<int, int[]> = Vessel.batch 3
```

### Composing vessels

Two vessels can be composed into one with `Vessel.compose` (or the `>>>` operator):

```fsharp
let doubledEvens : Vessel<int, int> =
    Vessel.map (fun x -> x * 2) >>> Vessel.filter (fun x -> x % 2 = 0)
```

Composition is lazy — no work happens until the stream is consumed.

> **Operator scope:** `>>>` is defined inside `module Vessel` and is
> available when that module is open. When calling from a context where
> the module is not open, use `Vessel.compose` directly:
>
> ```fsharp
> let vessel = Vessel.compose (Vessel.map (fun x -> x * 2)) (Vessel.filter (fun x -> x % 2 = 0))
> ```

### Connecting root, vessel, and leaf

Use `Pipeline.runWith` to wire all three together:

```fsharp
do! Pipeline.runWith root vessel leaf
```

`Pipeline.runWith` threads the stream through the vessel before handing
it to the leaf:

```fsharp
let runWith (root: Root<'TIn>) (vessel: Vessel<'TIn,'TOut>) (leaf: Leaf<'TOut>) : Task<unit> =
    leaf.Write(vessel.Transform(root.Read()))
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
    | RetryError            of attempt: int * exn
```

`RetryError` is emitted by the retry engine (see *"Retry policy"* below)
and carries the 1-based attempt number alongside the exception. Callers
can pattern-match on it to distinguish retry-related diagnostics from
other failures.

**Adding a new well-known case is intentionally a breaking change.**
Exhaustive pattern matches will fail to compile, forcing every caller to
explicitly handle the new category. `Custom` is the safety valve when
you need a domain-specific kind without modifying the library.

### `Pulse`

A single structured event emitted during a pipeline run:

```fsharp
type Pulse = {
    Severity:    Severity
    Kind:        ErrorKind
    Stage:       string option        // which vessel/stage emitted this
    RecordIndex: int64 option         // 0-based record position, if applicable
    Timestamp:   DateTimeOffset
    Message:     string               // human-readable summary
}
```

`Stage` and `RecordIndex` are both `option` because not every event is
tied to a specific stage or record (e.g. a file-open failure has no
record index; a pre-flight config check has no stage).

### `Harvest`

The structured outcome of a completed pipeline run:

```fsharp
type Harvest = {
    RecordsRead:     int64
    RecordsAccepted: int64
    RecordsRejected: int64
    RecordsFailed:   int64
    Duration:        TimeSpan
    Events:          Pulse list
}
```

`Events` is the source of truth. The counts are pre-computed
conveniences — they are always consistent with `Events` and save callers
from folding the list themselves.

### `Ring`

A per-stage aggregation of diagnostic event counts:

```fsharp
type Ring = {
    Stage:        string option
    InfoCount:    int64
    WarningCount: int64
    ErrorCount:   int64
    FatalCount:   int64
    TotalCount:   int64
}
```

`Stage` mirrors `Pulse.Stage` — it is `Some "validate"` for
events emitted by a named vessel, or `None` for pipeline-level events such
as retry warnings or fatal exceptions caught by `runWithContext`.

#### Producing summaries

`Harvest.summarizeByStage` folds a `Pulse list` into a
`Ring list`:

```fsharp
let result = Harvest.fromEvents count duration (ctx.ReadEvents())
let summaries = Harvest.summarizeByStage result.Events
```

The returned list preserves **first-occurrence order** — summaries
appear in the order their stage was first seen in the event stream. This
gives callers a natural pipeline-order view without requiring stages to
carry explicit sequence numbers.

#### Typical usage

After a pipeline run, summaries answer questions like *"how many records
did the validate stage reject?"* without scanning the raw event list:

```fsharp
let summaries = Harvest.summarizeByStage result.Events

for s in summaries do
    let stage = s.Stage |> Option.defaultValue "(pipeline)"
    printfn $"{stage}: {s.ErrorCount} errors, {s.WarningCount} warnings"
```

Events with `Stage = None` are grouped together — they typically contain
retry diagnostics, fatal pipeline exceptions, or any other event not
tied to a specific vessel stage.

### Design decisions — `Ring`

#### Standalone function, not a `Harvest` field

`summarizeByStage` is a standalone function in the `Harvest`
module rather than a pre-computed field on the `Harvest` record.

The alternative — adding a `StageSummaries: Ring list` field to
`Harvest` and populating it in `fromEvents` — was considered but
rejected for several reasons:

1. **Not every caller needs summaries.** Pre-computing them on every run
   adds allocation and computation that simple pipelines (or pipelines
   that only check top-level counts) would never use.
2. **Record stability.** Adding a field to `Harvest` is a
   breaking change for anyone pattern-matching or constructing the
   record directly. A new function in the module is additive.
3. **Composability.** Callers can filter or transform the event list
   before summarizing (e.g. only summarize errors, or only events from
   a specific time window). A pre-computed field would force
   re-computation for those use cases.

The trade-off is that callers who *do* want summaries must call the
function explicitly. This is a minor inconvenience compared to the
flexibility gained.

#### Grouping by `string option`, not a `Stage` type

Stages are identified by their `string option` name, matching the
`Pulse.Stage` field. An alternative would be a dedicated
`Stage` type with richer metadata (ordering, parent pipeline, etc.).

This was deferred because:

1. The current model has no first-class `Stage` concept — stages are
   just names passed to `Vessel.validate`, `Vessel.enrich`, etc.
2. Introducing a `Stage` type would ripple through `ExecutionContext`,
   vessel combinators, and event construction — significant churn for
   marginal benefit at v1 scope.
3. String names are simple, debuggable, and sufficient for grouping.

If v2 introduces sub-pipelines or reusable fragments, a richer `Stage`
type may become worthwhile.

#### First-occurrence ordering

Summaries are ordered by the first time each stage appears in the event
list, not alphabetically or by event count. This was chosen because:

1. It naturally reflects pipeline execution order — the first stage to
   emit an event appears first in the summary.
2. It requires no explicit ordering metadata on stages.
3. Alphabetical ordering would scramble the pipeline flow; count-based
   ordering would vary between runs.

The trade-off is that a stage emitting no events has no summary entry.
This is consistent with the principle that summaries reflect *what
happened*, not *what was configured*. A future enhancement could accept
a list of known stage names and produce zero-count entries for quiet
stages, but this adds complexity without a clear use case today.

#### `int64` counts, not `int`

Counts use `int64` to stay consistent with `Harvest.RecordsRead`
and other counters. This avoids lossy conversions when comparing summary
counts against pipeline-level totals, and future-proofs against large
event volumes.

---

## `ExecutionContext`

An `ExecutionContext` coordinates a single pipeline run. It carries
everything a vessel or combinator needs at runtime — without baking
run-specific concerns into the `Vessel` type itself.

### Type definition

```fsharp
type ExecutionContext = {
    CancellationToken: System.Threading.CancellationToken
    BatchSize:         int
    Emit:              Pulse -> unit
    ReadEvents:        unit -> Pulse list
    RetryPolicy:       RetryPolicy
}
```

| Field | Purpose |
|---|---|
| `CancellationToken` | Signals cooperative cancellation; ctx-aware vessels check it per item |
| `BatchSize` | Preferred number of records per batch for batch-aware sinks and vessels |
| `Emit` | Records a `Pulse` for the current run (thread-safe) |
| `ReadEvents` | Returns all events emitted so far, in emission order |
| `RetryPolicy` | Controls retry behaviour on pipeline failure (default: `NoRetry`) |

### Creating a context

```fsharp
// Sensible defaults — CancellationToken.None, BatchSize 1 000
let ctx = ExecutionContext.``default`` ()

// Explicit token and batch size
use cts = new System.Threading.CancellationTokenSource()
let ctx = ExecutionContext.create cts.Token 500
```

`ExecutionContext.create` wires `Emit` and `ReadEvents` to the same
internal `ResizeArray`, guarded by a `lock`. This means `Emit` is
**thread-safe**: concurrent calls are serialised and emission order is
preserved. `ReadEvents` also acquires the lock and returns an
independent `list` snapshot — callers can read safely while other
threads continue emitting.

---

## Context-aware vessel combinators

Pure vessels (`map`, `filter`, `compose`) have no side effects and need no
context. Combinators that can *reject or fail records* receive an
`ExecutionContext` at construction time so they can emit structured
diagnostics without changing the `Vessel` type.

### `Vessel.validate`

Validates every item; passes `Ok` items downstream unchanged and drops
`Error` items, emitting one `Pulse` of severity `Error` per
rejection.

```fsharp
let vessel : Vessel<int, int> =
    Vessel.validate "check-positive" ctx (fun x ->
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

### `Vessel.enrich`

Enriches every item using a function that may change the record type.
`Ok` items are passed downstream as the enriched value; `Error` items
are dropped with an `Error` diagnostic — same pattern as `validate`.

```fsharp
// int -> string enrichment (type changes)
let vessel : Vessel<int, string> =
    Vessel.enrich "add-label" ctx (fun x ->
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

### `Vessel.batch`

`Vessel.batch` groups a stream of individual items into a stream of
fixed-size arrays. It is a pure structural transform — it needs no
`ExecutionContext` and emits no diagnostics.

```fsharp
// Groups items into arrays of at most 100
let vessel : Vessel<Row, Row[]> = Vessel.batch 100
```

#### Behaviour

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

#### Using `ctx.BatchSize`

`BatchSize` is a first-class field on `ExecutionContext` for exactly
this purpose. Pass it directly to respect the pipeline's configured
batch size:

```fsharp
let vessel = Vessel.batch ctx.BatchSize
```

This keeps the batch size in one place — the context — rather than
hard-coding it at each call site.

#### Output type: `'T[]`

`batch` emits `'T[]` (array), not `'T list` or `seq<'T>`. Arrays are:

- **Fixed-size** — the leaf knows exactly how many records it received
- **Contiguous** — optimal for bulk-insert APIs (SQL `SqlBulkCopy`,
  `IDataReader`, etc.)
- **Efficient** — `ResizeArray` is used internally; `ToArray()` is a
  single allocation per batch

`'T list` would be equally safe but adds an allocation and a traversal
for callers that need to hand the batch to a .NET API expecting an array.

#### Composing `batch` with other vessels

Because the output type changes from `'T` to `'T[]`, `batch` is
typically the **last** vessel in a composed pipeline:

```fsharp
// validate, then enrich, then group into batches for bulk insert
let vessel =
    Vessel.compose
        (Vessel.validate "check" ctx validator)
        (Vessel.compose
            (Vessel.enrich "enrich" ctx enricher)
            (Vessel.batch ctx.BatchSize))
```

A leaf that processes `'T[]` directly handles the batch as a unit:

```fsharp
let bulkLeaf : Leaf<Row[]> = {
    Write = fun batches -> task {
        for batch in batches do
            do! db.BulkInsertAsync(batch)
    }
}
```

---

## Running a pipeline with context

`Pipeline.runWithContext` is the full-featured runner. It wraps
`runWith`, counts every record emitted by the root, measures
wall-clock duration, and collects all diagnostic events from `ctx`:

```fsharp
let ctx = ExecutionContext.``default`` ()

let! result : Harvest = Pipeline.runWithContext ctx root vessel leaf
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
already-cancelled token throws immediately without touching the root.

### Unhandled exceptions — retries and guaranteed `Harvest`

`runWithContext` handles failures according to the `RetryPolicy` on the
context. With the default `NoRetry`, any unhandled exception is caught,
emitted as one `Fatal`-severity `Pulse`, and a well-formed
`Harvest` is returned reflecting the partial run.

When a `FixedDelay` retry policy is configured, the engine re-executes
the **entire pipeline** from scratch on each retry. Each failed attempt
emits a `Warning`-level `RetryError` event. If all retries are
exhausted, a `Fatal`-level `RetryError` event is emitted and the partial
result is returned:

```fsharp
// Retry up to 3 times with 500ms between attempts
let ctx = { ExecutionContext.``default`` () with RetryPolicy = FixedDelay(3, TimeSpan.FromMilliseconds 500.0) }

let! result = Pipeline.runWithContext ctx root vessel leaf

// Inspect retry diagnostics
for e in result.Events do
    match e.Kind with
    | RetryError (attempt, ex) ->
        printfn $"[%A{e.Severity}] Attempt {attempt}: {ex.Message}"
    | _ -> ()
```

See the *"Design decision: retry policy"* section below for the full
rationale.

**What this guarantees:**

| Failure scenario | Guarantee |
|---|---|
| Connector throws on open | `Harvest` with `RecordsFailed = 1` |
| Vessel throws mid-stream | Result with partial counts + `Fatal` event |
| Duration | Always measured, even on failure |
| Events emitted before crash | Included in `result.Events` |

> **Note:** `OperationCanceledException` (from `CancellationToken`) is
> intentionally **not** caught. A cancelled pipeline is not a pipeline
> failure — it is a deliberate stop signal, and the exception should
> propagate normally so callers can distinguish cancellation from error.

---

## Connectors

A **connector** is a `Root<'T>` or `Leaf<'T>` that ties the pipeline
to a specific data store or transport. The core library ships two connectors out of the box:
`Xylem.Connectors.InMemory` and `Xylem.Connectors.File`. JSON/CSV,
database, and queue connectors are planned for future releases.

### `Xylem.Connectors.InMemory`

The in-memory connector requires no I/O and no external dependencies.
It is the go-to choice for tests, examples, and simple one-off
pipelines.

```fsharp
open Xylem.Connectors
```

#### `InMemory.source`

Creates a `Root<'T>` from any `seq<'T>`-compatible value — lists,
arrays, and sequences all work:

```fsharp
let root = InMemory.source [1; 2; 3; 4; 5]
let root = InMemory.source [| "a"; "b"; "c" |]
let root = InMemory.source (seq { for i in 1..100 do yield i })
```

Each call to `root.Read()` produces a fresh, independent
`IAsyncEnumerable<'T>` — the root behaves exactly like any other
Xylem root. Multiple runs over the same root are safe as long as
the underlying sequence is re-iterable (lists and arrays always are;
one-shot `seq` expressions are not).

#### `InMemory.sink`

Creates a `Leaf<'T>` that accumulates every written item into an
internal buffer. Returns the sink together with a **reader function**
that snapshots the collected items on demand:

```fsharp
let leaf, read = InMemory.sink ()

do! Pipeline.runWith root vessel leaf

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
let root = InMemory.source [1; -2; 3; -4; 5]
let vessel = Vessel.validate "check-positive" ctx (fun x ->
    if x > 0 then Ok x
    else Result.Error (ValidationError("value", "must be positive")))
let leaf, read = InMemory.sink ()

let! result = Pipeline.runWithContext ctx root vessel leaf

printfn $"Accepted: %A{read ()}"          // [1; 3; 5]
printfn $"Rejected: %d{result.RecordsRejected}"  // 2
```

---

### Design decisions — `InMemory`

#### `#seq<'T>` vs `seq<'T>` for the root input

`InMemory.source` accepts `#seq<'T>` (a flexible type) rather than
`seq<'T>`. The difference: with `seq<'T>`, passing a `list` or
`array` boxes it into a plain `seq` before the function is entered —
the static type is lost. With `#seq<'T>`, the compiler accepts any
subtype of `IEnumerable<'T>` without an upcast, keeping the most
specific type at the call site.

In practice the difference is invisible at runtime, but `#seq<'T>` is
the idiomatic F# choice for functions that accept *any* sequence.

#### `unit -> 'T list` reader vs returning the list directly

`InMemory.sink` returns `Leaf<'T> * (unit -> 'T list)` rather than
`Leaf<'T> * 'T list`. If it returned the list directly, the list would
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

## Design decision: how vessels emit diagnostics

This decision is worth documenting in full because the alternatives have
non-obvious trade-offs.

### Option A — `Result` in the stream (rejected)

The most obviously functional approach: vessels return
`IAsyncEnumerable<Result<'TOut, Pulse>>` so rejections are
inline:

```fsharp
type Vessel<'TIn, 'TOut> = {
    Transform: IAsyncEnumerable<'TIn> -> IAsyncEnumerable<Result<'TOut, Pulse>>
}
```

**Why we didn't choose this:**

- **Type explosion on composition.**<br/>After chaining two vessels the return
  type becomes
  `IAsyncEnumerable<Result<Result<'C, Pulse>, Pulse>>`.
  A `bind`-style compose flattens it, but the ergonomics deteriorate
  quickly and the engine must understand the nesting.
- **Warnings are unrepresentable.**<br/>A record that *passes* validation but
  triggers a warning (e.g. a coerced null) must be `Ok` — there is no
  channel for "healthy record, but here is a note". You would need
  `Result<'TOut * Pulse list, Pulse list>`, which is
  a very complex return type.
- **Most vessels don't reject anything.**<br/>`map` and `filter` are pure
  transforms. Forcing all vessels to wrap their output in `Result` for the
  sake of a few validation vessels is a poor trade.

### Option B — `ExecutionContext` with `Emit` (chosen, implemented)

A context object is threaded through diagnostics-aware combinators:

```fsharp
type ExecutionContext = {
    CancellationToken: System.Threading.CancellationToken
    BatchSize:         int
    Emit:              Pulse -> unit
    ReadEvents:        unit -> Pulse list
    RetryPolicy:       RetryPolicy
}
```

Vessels that need to emit events receive a context at *construction time*,
not as part of the `Vessel` type itself:

```fsharp
// Pure vessel — no context needed, clean signature
let doubled = Vessel.map (fun x -> x * 2)

// Validating vessel — opts into context at construction time
let validateAge ctx =
    Vessel.validate "check-age" ctx (fun person ->
        if person.Age < 0 then
            Result.Error (ValidationError("Age", "must be >= 0"))
        else
            Ok person)
```

**Why this works:**

- `Vessel<'TIn,'TOut>` stays exactly as it is. No type changes, no
  breaking changes to existing combinators.
- Any event at any time: warnings on healthy records, multiple errors per
  record, informational events mid-stream — all natural.
- Pure vessels (`map`, `filter`, `compose`) remain completely side-effect
  free and need no context.
- Only vessels that *opt in* to diagnostics touch `ctx`.

**The drawback:** `ctx.Emit` is a side effect. Vessels that use it are no
longer purely functional — they produce output *and* write to the context.
This is a deliberate pragmatic choice. ETL pipelines inherently
produce side effects (writing files, hitting databases); pretending
diagnostics can be fully pure adds complexity without benefit.

`ExecutionContext` and the ctx-aware combinators (`validate`, `enrich`)
are now implemented. Pure vessels (`map`, `filter`, `compose`) remain
completely side-effect free and need no context.

---

## Design decision: exception handling in `runWithContext`

### The problem

`runWithContext` is the only place that can return a structured
`Harvest`. Before this decision was made, any unhandled exception
— from a connector opening a file, a vessel throwing mid-stream, a sink
failing to write — caused the `Task<Harvest>` itself to fault.
This meant:

- The structured result was never returned; callers had to use a raw
  `try/catch` to get anything.
- `RecordsFailed` was always 0, even on hard failures — the count is
  derived from `Fatal` events, which were never emitted.
- `Duration` was never measured.
- Events emitted *before* the crash were stranded in `ctx` and only
  reachable if the caller explicitly called `ctx.ReadEvents()` in a
  catch block — a non-obvious escape hatch.

### Three options considered

**Option A — leave exceptions unhandled (rejected)**

Connectors throw, callers wrap the runner in `try/catch`. Simple and
honest, but `Harvest` never reaches the caller on hard failures
and `Fatal` events serve no purpose for connector-level errors.

**Option B — catch in `runWithContext` (chosen)**

The engine wraps the inner run in `try/catch`. Any unhandled exception
is caught, emitted as a `Fatal` `Pulse`, and the runner returns
a well-formed `Harvest` reflecting the partial run. The `IoError`
case on `ErrorKind` — which existed in the model but was previously
unreachable — is now the natural carrier for connector I/O failures.

**Option C — connectors accept `ctx` and emit `Fatal` themselves (rejected)**

Connectors would be responsible for catching their own errors and emitting
diagnostics. This is consistent with how vessels handle per-record errors,
but it forces every connector to accept and thread an `ExecutionContext` —
complicating the `Root<'T>` / `Leaf<'T>` types and coupling connectors
to the diagnostics model for what are fundamentally infrastructure errors.

### Why Option B

- `Root<'T>` and `Leaf<'T>` stay context-free. Connectors have no
  dependency on `ExecutionContext`.
- `Harvest` is always returned — callers can always inspect
  `result.RecordsFailed` and `result.Events` regardless of how the run
  ended.
- `Fatal` in the diagnostics model becomes meaningful end-to-end: a
  connector I/O failure, a mid-stream vessel exception, and a cancelled
  run can all be distinguished by `Severity` and `Kind`.
- The catch is in one place only — the engine — not scattered across
  every connector.

### Cancellation is not caught

`OperationCanceledException` is deliberately excluded from the catch.
Cancellation is not a failure — it is a deliberate stop signal. Catching
it would suppress the caller's ability to detect that the pipeline was
cancelled rather than failed. The pre-existing
`ThrowIfCancellationRequested()` check at the top of `runWithContext`
continues to propagate normally. This also applies during retries —
cancellation between retry attempts (during the delay) propagates
immediately rather than being swallowed.

---

## Retry policy

### `RetryPolicy`

```fsharp
type RetryPolicy =
    | NoRetry
    | FixedDelay of maxAttempts: int * delay: TimeSpan
```

| Case | Behaviour |
|---|---|
| `NoRetry` | Failure is immediately final. This is the default. |
| `FixedDelay(n, d)` | On failure, wait `d`, then re-run the pipeline from scratch. Repeat up to `n` times. Total executions = `n + 1` (initial + retries). |

### Configuring retry

Set `RetryPolicy` on the `ExecutionContext` using a record update:

```fsharp
let ctx =
    { ExecutionContext.``default`` () with
        RetryPolicy = FixedDelay(3, TimeSpan.FromSeconds 1.0) }
```

### What gets retried

The retry loop wraps the **entire pipeline**: root → vessel → leaf.
On each retry, `root.Read()` is called again, the vessel processes from
the beginning, and the sink receives a fresh stream. The record count
is reset per attempt — `Harvest.RecordsRead` reflects only the
last (successful or final) attempt.

### Diagnostic events during retries

Each failed attempt emits a `Warning`-level event with `RetryError`:

```fsharp
{ Severity = Warning
  Kind     = RetryError(1, ex)   // 1-based attempt number
  Stage    = None
  ...
  Message  = "Attempt 1 failed: <message>. Retrying in 1000ms…" }
```

If all retries are exhausted, a `Fatal`-level `RetryError` is emitted:

```fsharp
{ Severity = Fatal
  Kind     = RetryError(4, ex)   // final attempt (initial + 3 retries)
  ...
  Message  = "Pipeline failed after 4 attempt(s): <message>" }
```

### Example: retry with inspection

This example configures a pipeline that retries up to twice on failure,
waiting 200ms between attempts. After the run, it prints the overall
counts and then extracts the retry-specific events from the result.
Because diagnostic events accumulate across all attempts, you get a
full history: a `Warning` for each failed-but-retried attempt, and — if
the pipeline never recovered — a final `Fatal` indicating exhaustion.

```fsharp
open System

let ctx =
    { ExecutionContext.``default`` () with
        RetryPolicy = FixedDelay(2, TimeSpan.FromMilliseconds 200.0) }

let! result = Pipeline.runWithContext ctx root vessel leaf

printfn $"Read: %d{result.RecordsRead}  Failed: %d{result.RecordsFailed}"

let retries =
    result.Events
    |> List.choose (fun e ->
        match e.Kind with
        | RetryError (attempt, _) -> Some (e.Severity, attempt)
        | _ -> None)

for (sev, attempt) in retries do
    printfn $"  [{sev}] attempt {attempt}"
```

---

## Design decision: retry policy

### The problem

`runWithContext` catches unhandled exceptions and returns a
`Harvest`, but the pipeline fails permanently on the first
error. Transient failures — network glitches, file locks, temporary
service unavailability — are common in ETL workloads and ideally
shouldn't require manual restarting.

### Three options considered

**Option A — per-record retry (rejected)**

Retry individual records that fail inside a vessel or sink. This is the
most granular approach and avoids re-reading the source.

*Why rejected:* Requires record-level buffering, replay infrastructure,
and checkpoint support. A failed record inside a `taskSeq` pipeline
can't simply be "replayed" without rewinding the async enumerator —
which `IAsyncEnumerable` doesn't support. This is v2 territory (paired
with checkpointing and dead-letter handling).

**Option B — whole-pipeline retry (chosen, implemented)**

On failure, re-execute the entire pipeline from `root.Read()`. The
root produces a fresh stream, the vessel transforms from scratch, and
the sink receives fresh output.

*Why chosen:*
- **Simple and predictable.** No buffering, no partial state, no
  enumerator rewinding. The pipeline is stateless by design — re-running
  it is the same as running it for the first time.
- **Correct for idempotent sources.** Files, databases with stable
  queries, and API endpoints with deterministic responses all produce the
  same data on re-read. This covers the vast majority of ETL sources.
- **Composes with existing guarantees.** The `try/catch` in
  `runWithContext` already handles exceptions uniformly. Retry is a loop
  around the same mechanism — no new error paths.
- **Diagnostic events accumulate across attempts.** `Warning` events from
  early attempts remain in the context alongside the final `Fatal` or
  success, giving full visibility into the retry history.

**Option C — external retry (caller-side) (rejected)**

Don't build retry into the engine. Let callers wrap `runWithContext` in
their own retry loop.

*Why rejected:* Callers would need to re-create the `ExecutionContext`
(to reset events) or manage event accumulation themselves. The retry
logic is tightly coupled to the diagnostic emission model — the engine
is the natural place for it.

### Trade-offs of whole-pipeline retry

| Concern | Assessment |
|---|---|
| Non-idempotent sources | Re-reading may produce different data or trigger side effects. Callers with non-idempotent sources should use `NoRetry`. |
| Sink side effects | A sink that already wrote partial output before the failure will receive a fresh stream on retry. Sinks that append (e.g. `File.sink` with `Append = true`) may duplicate records. Overwrite-by-default sinks are safe. |
| Performance | Re-reading the full root is wasteful if the failure happened near the end. Acceptable at v1; per-record retry with checkpointing is a v2 concern. |
| Event accumulation | Events from failed attempts persist in the context. This is intentional — they provide retry history — but callers should be aware that `result.Events` may contain `Warning`-level `RetryError` events from earlier attempts. |

### Why `FixedDelay` only (for now)

Exponential backoff, jitter, and circuit-breaker patterns are valuable
for production resilience but add configuration surface and testing
complexity. `FixedDelay` covers the 80% case (transient I/O failures
with a short pause) and is easy to reason about. The `RetryPolicy` DU
is designed for extension — adding `ExponentialBackoff of maxAttempts *
initialDelay * multiplier` in v2 is a non-breaking change.

### Why `Emit` is synchronous and thread-safe

The `Emit` field on `ExecutionContext` is `Pulse -> unit`
rather than `Pulse -> Task<unit>`. This was a deliberate
choice:

- **Ergonomics.** Every call site in `validate`, `enrich`, and the retry
  loop would need `do! ctx.Emit ...` instead of `ctx.Emit ...`. The
  async friction compounds across every ctx-aware combinator.
- **Current consumers are in-memory.** Events are collected into a
  `ResizeArray` — an inherently synchronous operation. There is no async
  work to perform.
- **Thread-safety is sufficient.** The `lock`-guarded `ResizeArray`
  handles concurrent access safely. Under contention, the lock serialises
  writes without deadlock risk (the critical section is a single `Add`).
- **Future async emission.** If v2 needs to stream events to an external
  sink (e.g. a logging service), an `EmitAsync: Pulse ->
  Task<unit>` field can be added alongside `Emit` without breaking
  existing callers. The engine can call `EmitAsync` when present and fall
  back to `Emit` otherwise.

---
## File Connectors

### `Xylem.Connectors.File`

The file connector reads from and writes to the local file system.
Like all Xylem connectors, it lives in `Xylem.Connectors`:

```fsharp
open Xylem.Connectors
```

#### Responsibility boundary — lines only

`File.source` produces `string` lines. `File.sink` consumes `string`
lines. **Parsing and serialisation are not the connector's job** — they
belong in a `Vessel` sitting between root and leaf.

This keeps each piece focused:

```
File.source "input.csv"
  → Vessel.map parseCsvRow       // string → Row
  → Vessel.validate "check" ctx validator
  → Vessel.map formatCsvRow      // Row → string
  → File.sink "output.csv"
```

A connector that bundled its own CSV or JSON parser would duplicate the
format connectors and force every caller to use its specific parser
whether they wanted to or not.

#### `File.sourceFrom` — factory constructor

The primary constructor. Accepts a factory function that produces a
`TextReader` and wraps it as a `Root<string>`:

```fsharp
File.sourceFrom : (unit -> TextReader) -> Root<string>
```

Each call to `root.Read()` invokes the factory to obtain a fresh
`TextReader`, yields its lines one at a time, and disposes the reader
when enumeration ends. The factory is called lazily — only when the
first item is pulled from the stream.

This is the overload to use in tests (see *Testing without I/O* below).

#### `File.source` — convenience overload

Wraps `sourceFrom` with a `StreamReader` factory for a file path:

```fsharp
let root : Root<string> = File.source "data.csv"
```

Equivalent to:

```fsharp
let root = File.sourceFrom (fun () -> new StreamReader("data.csv"))
```

Each call to `root.Read()` opens a fresh `StreamReader`, streams
lines one at a time, and disposes the reader on completion, cancellation,
or exception. The file is never fully loaded into memory.

Empty lines are yielded as empty strings — they are not skipped. Use
`Vessel.filter (fun line -> line <> "")` to drop them upstream.

If the file does not exist or cannot be opened, the `StreamReader`
constructor throws during the first iteration. The exception is caught
by `Pipeline.runWithContext`, which emits a `Fatal` diagnostic and
returns a well-formed `Harvest`.

#### `File.sinkFrom` — factory constructor

The primary constructor. Accepts a factory function that produces a
`TextWriter` and wraps it as a `Leaf<string>`:

```fsharp
File.sinkFrom : (unit -> TextWriter) -> Leaf<string>
```

`Write` invokes the factory once, writes each string as a line via
`TextWriter.WriteLine`, then disposes the writer — whether the stream
completes normally or throws.

This is the overload to use in tests (see *Testing without I/O* below).

#### `File.sink` — convenience overloads

Wraps `sinkFrom` with a `StreamWriter` factory for a file path:

```fsharp
// Overwrite (default) — one-arg convenience
let leaf : Leaf<string> = File.sinkDefault "output.csv"

// With explicit options
let leaf = File.sink "output.csv" { FileLeafOptions.Default with Append = true }
```

The default behaviour **overwrites** the file if it already exists —
pipelines are designed to be re-runnable, and appending to a previous
run's output would produce corrupt data. Append is opt-in via
`FileLeafOptions`.

#### `FileLeafOptions`

```fsharp
type FileLeafOptions = {
    Append:   bool
    Encoding: System.Text.Encoding
}
```

| Field | Default | Purpose |
|---|---|---|
| `Append` | `false` | When `true`, new lines are appended to an existing file rather than overwriting it |
| `Encoding` | `UTF8` (no BOM) | Character encoding for the output file |

#### Resource lifetime

| Connector | `TextReader`/`TextWriter` opened | Closed |
|---|---|---|
| `File.sourceFrom` | At first `MoveNextAsync()` call | When enumeration ends (completion, cancellation, or exception) |
| `File.sinkFrom` | At the start of `Write(stream)` | When `Write` returns (success or exception) |

Both use `use` bindings so disposal is guaranteed regardless of how the
stream terminates. Multiple calls to `root.Read()` are safe — each
call invokes the factory independently.

#### Testing without I/O

Because both primary constructors accept a factory function, tests
substitute `StringReader` and `StringWriter` — standard .NET types —
in place of real file handles. No temp files, no cleanup, no
environment dependencies:

```fsharp
// Source — inject a StringReader
let root = File.sourceFrom (fun () -> new StringReader("alice\nbob\ncarol"))

let! lines = root.Read() |> TaskSeq.toListAsync
// lines = ["alice"; "bob"; "carol"]

// Sink — inject a StringWriter and inspect what was written
let sw   = new StringWriter()
let leaf = File.sinkFrom (fun () -> sw :> TextWriter)

do! leaf.Write(taskSeq { yield "x"; yield "y" })
// sw.ToString() = "x\r\ny\r\n"  (or "x\ny\n" on Unix)
```

The convenience overloads (`File.source path`, `File.sink path`) are
integration-tested separately as thin wrappers over `sourceFrom`/`sinkFrom`.

#### Full pipeline example

```fsharp
open Xylem
open Xylem.Connectors
open Xylem.Domain

type Row = { Name: string; Age: int }

let parseLine (line: string) : Result<Row, ErrorKind> =
    match line.Split(',') with
    | [| name; age |] ->
        match System.Int32.TryParse(age) with
        | true, n -> Ok { Name = name.Trim(); Age = n }
        | _       -> Result.Error (ValidationError("Age", "not a valid integer"))
    | _ -> Result.Error (ValidationError("line", "expected 2 comma-separated fields"))

let formatLine (row: Row) : string = $"{row.Name},{row.Age}"

let ctx = ExecutionContext.``default`` ()

let root = File.source "people.csv"
let vessel =
    Vessel.compose
        (Vessel.enrich "parse" ctx parseLine)
        (Vessel.compose
            (Vessel.validate "check-age" ctx (fun row ->
                if row.Age >= 0 then Ok row
                else Result.Error (ValidationError("Age", "must be non-negative"))))
            (Vessel.map formatLine))
let leaf = File.sinkDefault "people-clean.csv"

let! result = Pipeline.runWithContext ctx root vessel leaf

printfn $"Read: %d{result.RecordsRead}  Accepted: %d{result.RecordsAccepted}  Rejected: %d{result.RecordsRejected}"
```

---

### Design decisions — `File`

#### Lines only, not generic

The alternative would be `File.source<'T>` with a built-in
`string -> 'T` deserialiser parameter. This conflates connector and
format concerns: the connector would need to know about CSV, JSON, or
whatever format the caller chooses.

Keeping connectors as `Root<string>` / `Leaf<string>` means format
handling belongs in a `Vessel`, which is the correct abstraction for
record-level transforms. The upcoming JSON and CSV connectors will
follow the same principle — they will be thin wrappers that compose a
`File.source` with a parsing vessel.

#### Factory functions for testability

Accepting `unit -> TextReader` and `unit -> TextWriter` rather than a
raw path keeps the connector testable without any mocking framework or
file system abstraction layer. `StringReader` and `StringWriter` are
standard .NET types that implement `TextReader` and `TextWriter`
respectively — no new interfaces or dependencies are needed.

The convenience path-based overloads (`File.source`, `File.sink`) are
thin wrappers that supply the `StreamReader`/`StreamWriter` factory.
They carry no logic of their own, so unit tests can focus entirely on
the `sourceFrom`/`sinkFrom` behaviour.

#### Overwrite by default

Append-by-default would silently corrupt output on a rerun.
Overwrite-by-default makes pipelines idempotent and rerunnable without
manual cleanup. Append is opt-in via `FileLeafOptions`.

#### `UTF-8` without BOM

UTF-8 without BOM is the cross-platform default. BOM causes problems
with many Unix tools and some parsers. Callers that need BOM or a
different encoding can supply a custom `Encoding` via `FileLeafOptions`.

#### Context-free connectors

`File.source` and `File.sink` do not accept an `ExecutionContext`. I/O
failures at the connector level (file not found, permission denied) are
fatal and unrecoverable — they are not per-record events. The engine's
`try/catch` in `runWithContext` handles them uniformly, emitting a
`Fatal` diagnostic and returning a well-formed `Harvest`. See the
*"Design decision: exception handling in `runWithContext`"* section for
full details.
