# 🌿 Xylem
**Xylem** is an ETL library designed to move data with the same elegance and
efficiency that plants move water.
> From root to result.

## Quickstart

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download) or later

### 1. Add Xylem to your project

```bash
dotnet add reference path/to/Xylem.fsproj
```

### 2. Your first pipeline

A pipeline wires a `Root` (data source) through a `Vessel` (transform) into a
`Leaf` (data sink). Nothing runs until the leaf pulls from the root — the whole
chain is lazy.

```fsharp
open Xylem.Domain
open Xylem.Connectors

// Create a root that produces integers
let root = InMemory.source [1; 2; 3; 4; 5]

// Create a leaf that collects results — `read` returns a snapshot
let leaf, read = InMemory.sink ()

// Build a vessel that doubles each value, then keeps only values > 4
let vessel =
    Vessel.map (fun x -> x * 2)
    >>> Vessel.filter (fun x -> x > 4)

// Run the pipeline
do! Pipeline.runWith root vessel leaf

let results = read ()  // [6; 8; 10]
```

### 3. Validation with diagnostics

Use `Vessel.validate` to reject invalid records explicitly. Rejections are
captured as structured `Pulse` events — you always know which records failed,
at which stage, and why.

```fsharp
let ctx = ExecutionContext.``default`` ()
let root = InMemory.source [-1; 2; -3; 4; 5]

let vessel =
    Vessel.validate "check-positive" ctx (fun x ->
        if x > 0 then Ok x
        else Error (ValidationError("value", "must be positive")))

let leaf, read = InMemory.sink ()
let! result = Pipeline.runWithContext ctx root vessel leaf

printfn "read=%d accepted=%d rejected=%d"
    result.RecordsRead result.RecordsAccepted result.RecordsRejected
// read=5 accepted=3 rejected=2
```

### 4. File I/O

Swap `InMemory` for `File` and the rest of your pipeline stays untouched.
File roots produce one `string` per line; file leaves write one line per record.

```fsharp
let root = File.source "input.txt"
let leaf = File.sinkDefault "output.txt"

let vessel =
    Vessel.map (fun (line: string) -> line.Trim())
    >>> Vessel.filter (fun line -> line.Length > 0)

do! Pipeline.runWith root vessel leaf
```

### 5. Batching and enrichment

```fsharp
// Chunk records into arrays of 100
let batched = Vessel.batch 100

// Enrich records — like map, but can fail with a structured error
let enriched =
    Vessel.enrich "lookup" ctx (fun record ->
        match lookupExternalData record with
        | Some data -> Ok { record with Extra = data }
        | None -> Error (BusinessRuleViolation("lookup", "not found")))
```

### Running the tests

```bash
dotnet test
```

---

## What's in the box

Xylem gives you a small set of composable building blocks:

| Concept | What it does |
|---------|-------------|
| **Root&lt;'T&gt;** | Produces a stream of records — each read is independent |
| **Vessel&lt;'TIn, 'TOut&gt;** | Transforms records from one shape to another |
| **Leaf&lt;'T&gt;** | Consumes records and owns the iteration |
| **Pipeline** | Wires a Root → Vessel(s) → Leaf together |
| **ExecutionContext** | Carries runtime config: batch size, cancellation, diagnostics |
| **Harvest** | Structured outcome with counts, timing, and diagnostic events |

### Transforms

`map` · `filter` · `validate` · `batch` · `enrich` — all composable via `>>>`.

### Connectors

| Connector | Purpose |
|-----------|---------|
| **InMemory** | Root and leaf backed by plain sequences — great for tests |
| **File** | Line-oriented file root and leaf with encoding and append/overwrite options |

### Diagnostics

Every pipeline run produces structured `Pulse` values with severity levels
(Info / Warning / Error / Fatal), typed error kinds, stage names, record indices, and
timestamps. Counts for read, accepted, rejected, and failed items are a simple fold
over the event stream. Per-stage summaries are available via `Ring` values.

## Current status

Xylem is **mid-v1** — the core pipeline model, execution engine, diagnostics, basic
transforms, and first connectors are implemented and covered by 83 passing tests.

### What's done

- ✅ Core domain model (`Root`, `Vessel`, `Leaf`, `Pipeline`, `ExecutionContext`, `Harvest`)
- ✅ Structured diagnostics (`Pulse`, `Ring`) with severity, error kinds, and timing
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
- ⬜ Error handling and connector authoring guides

See [ROADMAP.md](ROADMAP.md) for the full v1 and v2 plan, and
[CHECKLIST.md](CHECKLIST.md) for detailed progress tracking.

## Guidelines

- Written in idiomatic F# — discriminated unions, `Result`, and pure functions where possible.
- Uses modern F# and .NET features.
- Correctness, reliability, and diagnostics over raw performance.
