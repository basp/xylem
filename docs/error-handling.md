# Error Handling Guide

How Xylem handles failures — from per-record rejections to pipeline-level crashes — and how to work with the results.

---

## Philosophy

Xylem treats errors as **data, not exceptions**. Every failure is captured
as a structured `Pulse` with a severity, an `ErrorKind`, a stage name, and
a record index. The pipeline keeps running past recoverable errors so you
always get a complete picture of what went wrong — not just the first failure.

Three principles guide the design:

1. **Failures are loud.** No record is silently dropped. Every rejection or
   error produces a `Pulse` that ends up in the `Harvest`.
2. **Structured, not stringly-typed.** `ErrorKind` is a discriminated union
   you can pattern-match — no parsing error messages.
3. **The pipeline always returns a result.** Even a crashing pipeline
   returns a well-formed `Harvest` with partial counts, timing, and the
   events emitted before the crash.

---

## Severity levels

Every `Pulse` carries a `Severity` that tells you how bad it is:

| Severity | Meaning | Pipeline continues? |
|----------|---------|---------------------|
| `Info` | Something noteworthy; pipeline is healthy | Yes |
| `Warning` | Unexpected but recoverable (e.g. a retry attempt) | Yes |
| `Error` | A record was rejected — it is dropped from the stream | Yes |
| `Fatal` | The pipeline cannot continue — execution is aborted | No |

The `Harvest` counts are derived directly from these:

- `RecordsRejected` = number of `Error`-severity pulses
- `RecordsFailed` = number of `Fatal`-severity pulses
- `RecordsAccepted` = `RecordsRead` − `RecordsRejected` − `RecordsFailed`

---

## Record-level errors: validate and enrich

The most common error path is a **record rejection** — a single record
fails validation or enrichment and is dropped from the stream while
the rest continue.

### Validation

`Vessel.validate` checks every record against a predicate that returns
`Result<'T, ErrorKind>`. Records that return `Ok` pass through; records
that return `Error` are dropped and a `Pulse` is emitted:

```fsharp
let ctx = ExecutionContext.``default`` ()

let vessel =
    Vessel.validate "check-age" ctx (fun person ->
        if person.Age >= 0 then Ok person
        else Error (ValidationError("Age", "must be non-negative")))
```

Each rejection emits a pulse with:
- `Severity = Error`
- `Kind` = the `ErrorKind` you returned
- `Stage = Some "check-age"` (the stage name you provided)
- `RecordIndex` = the 0-based position in the stream

### Enrichment

`Vessel.enrich` works the same way but can **change the record type**.
Use it for lookups, projections, or joins that might fail:

```fsharp
let vessel =
    Vessel.enrich "resolve-customer" ctx (fun orderId ->
        match lookupCustomer orderId with
        | Some customer -> Ok customer
        | None -> Error (BusinessRuleViolation("resolve-customer", $"no customer for order {orderId}")))
```

Both `validate` and `enrich` respect `CancellationToken` — they call
`ThrowIfCancellationRequested()` before processing each record.

### Choosing an ErrorKind

Pick the `ErrorKind` case that best describes what went wrong:

| Case | When to use |
|------|-------------|
| `ValidationError (field, reason)` | A field-level check failed |
| `BusinessRuleViolation (rule, reason)` | A domain rule was violated |
| `IoError (path, exn)` | An I/O operation failed |
| `SystemError exn` | An unexpected system-level error |
| `PipelineError (stage, exn)` | A stage-level infrastructure error |
| `Custom (tag, data)` | Domain-specific errors that don't fit the above |

For the full type definitions, see [diagnostics](diagnostics.md).

`Custom` is the extension point — use it freely for domain-specific
error categories without modifying the library:

```fsharp
Error (Custom("rate-limit", Map.ofList [("endpoint", url); ("retryAfter", "30")]))
```

---

## Pipeline-level errors

Not all failures are per-record. A connector might fail to open a file,
a vessel might throw an unhandled exception, or a sink might crash
mid-write. These are **pipeline-level** errors.

### Guaranteed Harvest

`Pipeline.runWithContext` catches any unhandled exception (except
`OperationCanceledException`), emits a `Fatal` pulse, and returns a
well-formed `Harvest`:

```fsharp
let ctx = ExecutionContext.``default`` ()
let root = File.source "missing-file.txt"
let vessel = Vessel.map id
let leaf, _ = InMemory.sink ()

let! result = Pipeline.runWithContext ctx root vessel leaf

// result.RecordsFailed = 1
// result.Events contains one Fatal pulse
```

This means you never have to wrap `runWithContext` in a `try/catch` to
get diagnostic information. The `Harvest` is always returned, even on
hard failure.

| Failure scenario | What you get |
|-----------------|--------------|
| Connector throws on open | `Harvest` with `RecordsFailed = 1`, one `Fatal` event |
| Vessel throws mid-stream | Partial counts + `Fatal` event |
| All records rejected | `RecordsFailed = 0`, `RecordsRejected = N` — not a crash |
| Duration | Always measured, even on failure |
| Events emitted before crash | All included in `result.Events` |

### Cancellation is not an error

`OperationCanceledException` is deliberately **not** caught. Cancellation
is a deliberate stop signal, not a failure. It propagates normally so
callers can distinguish "I stopped it" from "it broke":

```fsharp
use cts = new CancellationTokenSource()
let ctx = ExecutionContext.create cts.Token 1000

// Cancel before running
cts.Cancel()

// This throws OperationCanceledException — not a Harvest with Fatal
let! result = Pipeline.runWithContext ctx root vessel leaf
```

---

## Retry policy

When a pipeline fails, the retry policy controls what happens next.

### NoRetry (default)

The failure is immediately final. One `Fatal` pulse is emitted and the
partial `Harvest` is returned:

```fsharp
let ctx = ExecutionContext.``default`` ()  // RetryPolicy = NoRetry
```

### FixedDelay

Retries up to `maxAttempts` times with a constant delay between attempts.
The total number of executions is `maxAttempts + 1` (initial + retries):

```fsharp
let ctx =
    { ExecutionContext.``default`` () with
        RetryPolicy = FixedDelay(3, TimeSpan.FromMilliseconds 500.0) }
```

On each retry:
- The **entire pipeline** is re-executed from scratch (root, vessel, leaf)
- A `Warning`-level `RetryError` pulse is emitted with the attempt number
- Record counts are reset — only the final attempt's counts are reported

If all retries are exhausted:
- A `Fatal`-level `RetryError` pulse is emitted
- The `Harvest` reflects the last attempt's partial counts

### Inspecting retry events

```fsharp
let! result = Pipeline.runWithContext ctx root vessel leaf

for e in result.Events do
    match e.Severity, e.Kind with
    | Warning, RetryError (attempt, ex) ->
        printfn $"Attempt {attempt} failed: {ex.Message} — retrying"
    | Fatal, RetryError (attempt, ex) ->
        printfn $"Gave up after {attempt} attempt(s): {ex.Message}"
    | _ -> ()
```

### Cancellation during retries

If the `CancellationToken` is triggered during the delay between retries,
`OperationCanceledException` propagates immediately — the retry loop
does not swallow cancellation.

---

## Inspecting results

### Harvest counts

After a pipeline run, the `Harvest` gives you the summary:

```fsharp
let! result = Pipeline.runWithContext ctx root vessel leaf

printfn $"Read:     {result.RecordsRead}"
printfn $"Accepted: {result.RecordsAccepted}"
printfn $"Rejected: {result.RecordsRejected}"
printfn $"Failed:   {result.RecordsFailed}"
printfn $"Duration: {result.Duration}"
```

### Pulse list

The full event stream is in `result.Events`. Filter and pattern-match
to find what you need:

```fsharp
// All rejections from a specific stage
let ageErrors =
    result.Events
    |> List.filter (fun e -> e.Stage = Some "check-age" && e.Severity = Error)

// Which record indices were rejected?
let rejectedIndices =
    ageErrors |> List.choose (fun e -> e.RecordIndex)
```

### Per-stage summaries (Ring)

Use `Harvest.summarizeByStage` to aggregate pulse counts by stage:

```fsharp
let summaries = Harvest.summarizeByStage result.Events

for s in summaries do
    let stage = s.Stage |> Option.defaultValue "(pipeline)"
    printfn $"{stage}: {s.ErrorCount} errors, {s.WarningCount} warnings"
```

Summaries appear in **first-occurrence order** — the first stage to emit
a pulse appears first in the list. Stages that emit no pulses have no
entry.

---

## Patterns and recipes

### Validate-then-enrich pipeline

A common pattern: validate inputs, then enrich the valid ones. Rejections
from either stage are tracked independently:

```fsharp
let ctx = ExecutionContext.``default`` ()

let vessel =
    Vessel.validate "check-input" ctx (fun x ->
        if x > 0 then Ok x
        else Error (ValidationError("value", "must be positive")))
    >>> Vessel.enrich "add-label" ctx (fun x ->
        if x < 100 then Ok $"item-{x}"
        else Error (BusinessRuleViolation("add-label", "value too large")))

let! result = Pipeline.runWithContext ctx root vessel leaf

// Per-stage breakdown
let summaries = Harvest.summarizeByStage result.Events
// summaries[0] = { Stage = Some "check-input"; ErrorCount = ...; ... }
// summaries[1] = { Stage = Some "add-label";   ErrorCount = ...; ... }
```

### Custom error kinds for domain logic

Use `Custom` to carry domain-specific error data without modifying the
library:

```fsharp
type RateLimitInfo = { Endpoint: string; RetryAfter: int }

let enricher (order: Order) =
    match callApi order with
    | ApiOk data -> Ok { order with Details = data }
    | ApiRateLimited info ->
        Error (Custom("rate-limit",
            Map.ofList [
                ("endpoint", info.Endpoint)
                ("retryAfter", string info.RetryAfter)
            ]))
    | ApiNotFound ->
        Error (BusinessRuleViolation("api-lookup", $"order {order.Id} not found"))
```

### Reacting to fatal errors

Check `RecordsFailed` to decide whether the run was catastrophic:

```fsharp
let! result = Pipeline.runWithContext ctx root vessel leaf

if result.RecordsFailed > 0 then
    let fatal = result.Events |> List.filter (fun e -> e.Severity = Fatal)
    for e in fatal do
        eprintfn $"FATAL: {e.Message}"
    // Alert, retry with different config, etc.
else
    printfn $"Success: {result.RecordsAccepted} records processed"
```

---

## Summary

| Concept | Mechanism |
|---------|-----------|
| Record rejected by validation | `Error`-severity `Pulse`, record dropped, pipeline continues |
| Record rejected by enrichment | Same as validation |
| Unhandled exception in pipeline | Caught, `Fatal` pulse emitted, `Harvest` returned |
| Cancellation | `OperationCanceledException` propagates — not a `Harvest` |
| Retry on failure | `FixedDelay` re-executes pipeline, `Warning` per attempt |
| All retries exhausted | `Fatal` pulse, partial `Harvest` returned |
| Inspecting results | `Harvest` counts, `Pulse` list, `Ring` summaries |
