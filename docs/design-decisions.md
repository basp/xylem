# 📐 Design Decisions

Rationale behind key architectural choices in Xylem — the trade-offs, 
alternatives considered, and why each option was chosen.

---

## Ring

### Standalone function, not a `Harvest` field

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

### Grouping by `string option`, not a `Stage` type

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

### First-occurrence ordering

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

### `int64` counts, not `int`

Counts use `int64` to stay consistent with `Harvest.RecordsRead`
and other counters. This avoids lossy conversions when comparing summary
counts against pipeline-level totals, and future-proofs against large
event volumes.

---

## How vessels emit diagnostics

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

## Exception handling in `runWithContext`

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
