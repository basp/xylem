# Execution Context

The runtime coordinator for a pipeline run — carrying cancellation, batch size, diagnostic emission, and retry policy.

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

For the design rationale on how vessels emit diagnostics (and why
`ExecutionContext` was chosen over `Result`-in-the-stream), see
[design decisions — vessel diagnostics](design-decisions.md#how-vessels-emit-diagnostics).
