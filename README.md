# 🌿 Xylem
**Xylem** is an ETL library designed to move data with the same elegance and 
efficiency that plants move water. 
> From root to result.

## What's in the box

Xylem gives you a small set of composable building blocks:

| Concept | What it does |
|---------|-------------|
| **Source&lt;'T&gt;** | Produces a stream of records — each read is independent |
| **Flow&lt;'TIn, 'TOut&gt;** | Transforms records from one shape to another |
| **Sink&lt;'T&gt;** | Consumes records and owns the iteration |
| **Pipeline** | Wires a Source → Flow(s) → Sink together |
| **ExecutionContext** | Carries runtime config: batch size, cancellation, diagnostics |
| **PipelineResult** | Structured outcome with counts, timing, and diagnostic events |

### Transforms

`map` · `filter` · `validate` · `batch` · `enrich` — all composable via `|>`.

### Connectors

| Connector | Purpose |
|-----------|---------|
| **InMemory** | Source and sink backed by plain sequences — great for tests |
| **File** | Line-oriented file source and sink with encoding and append/overwrite options |

### Diagnostics

Every pipeline run produces structured `DiagnosticEvent` values with severity levels
(Info / Warning / Error / Fatal), typed error kinds, stage names, record indices, and
timestamps. Counts for read, accepted, rejected, and failed items are a simple fold
over the event stream.

## Examples

### Transforming a stream

Flows compose with `>>>`. No work happens until the sink pulls from the source —
the whole chain is lazy. `InMemory` connectors are handy for tests and quick
experiments without touching the file system.

```fsharp
let source = InMemory.source [1; 2; 3; 4; 5]
let sink, read = InMemory.sink ()

let flow =
    Flow.map (fun x -> x * 2)
    >>> Flow.filter (fun x -> x > 4)

do! Pipeline.runWith source flow sink

let results = read ()  // [6; 8; 10]
```

### Validating records — and knowing what was rejected

`validate` lets invalid records fail explicitly rather than silently disappear.
Rejected records are counted and recorded as structured `DiagnosticEvent` values —
you always know which records failed, at which stage, and why. The valid records
continue through the pipeline unchanged.

```fsharp
let ctx = ExecutionContext.``default`` ()
let source = InMemory.source [-1; 2; -3; 4; 5]

let validate =
    Flow.validate "check-positive" ctx (fun x ->
        if x > 0 then Ok x
        else Error (ValidationError("value", "must be positive")))

let sink, read = InMemory.sink ()
let! result = Pipeline.runWithContext ctx source validate sink

printfn "read=%d accepted=%d rejected=%d"
    result.RecordsRead result.RecordsAccepted result.RecordsRejected
// read=5 accepted=3 rejected=2
```

### Reading from a file and writing the results back

File sources produce one string per line. Flows are the same regardless of
connector — swap `InMemory` for `File` and the rest of your pipeline stays
untouched.

```fsharp
let source = File.source "input.txt"
let sink   = File.sinkDefault "output.txt"

let flow =
    Flow.map (fun (line: string) -> line.Trim())
    >>> Flow.filter (fun line -> line.Length > 0)

do! Pipeline.runWith source flow sink
```

## Current status

Xylem is **mid-v1** — the core pipeline model, execution engine, diagnostics, basic
transforms, and first connectors are implemented and covered by ~80 passing tests.

### What's done

- ✅ Core domain model (`Source`, `Flow`, `Sink`, `Pipeline`, `ExecutionContext`, `PipelineResult`)
- ✅ Structured diagnostics with severity, error kinds, and timing
- ✅ `Result`-based error handling with rejection paths
- ✅ Async execution with cancellation support
- ✅ Transforms: map, filter, validate, batch, enrich
- ✅ In-memory connector (for testing and examples)
- ✅ File connector (line-oriented read/write)
- ✅ Integration, diagnostics, and failure-path tests

### What's next

- ✅ Basic retry policy (with thread-safe diagnostics)
- ⬜ Simple routing / branching
- ✅ Per-stage diagnostic summaries
- ⬜ JSON and CSV connectors
- ⬜ Quickstart, error handling, and connector authoring guides

See [ROADMAP.md](ROADMAP.md) for the full v1 and v2 plan, and
[CHECKLIST.md](CHECKLIST.md) for detailed progress tracking.

## Guidelines

- Written in idiomatic F# — discriminated unions, `Result`, and pure functions where possible.
- Uses modern F# and .NET features.
- Correctness, reliability, and diagnostics over raw performance.
