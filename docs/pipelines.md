# Running Pipelines

> How to execute a complete Root → Vessel → Leaf pipeline and interpret
> the results.

---

## `Pipeline.runWithContext`

`Pipeline.runWithContext` is the full-featured runner. It wraps
`runWith`, counts every record emitted by the root, measures
wall-clock duration, and collects all diagnostic events from `ctx`:

```fsharp
let ctx = ExecutionContext.``default`` ()

let! result : Harvest = Pipeline.runWithContext ctx root vessel leaf
```

`result.RecordsRead` is the direct count of records emitted by the root.
`result.RecordsRejected` and `result.RecordsFailed` are derived from the
events in `ctx`, and `result.RecordsAccepted` is computed as
`RecordsRead - RecordsRejected - RecordsFailed`. These counts are always
consistent with `result.Events`.

```fsharp
printfn $"Read: %d{result.RecordsRead}  Accepted: %d{result.RecordsAccepted}  Rejected: %d{result.RecordsRejected}"
for e in result.Events do
    printfn $"[%A{e.Severity}] stage=%A{e.Stage} index=%A{e.RecordIndex} — %s{e.Message}"
```

The runner checks `ctx.CancellationToken` before starting so that an
already-cancelled token throws immediately without touching the root.

---

## Unhandled exceptions — retries and guaranteed `Harvest`

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

See [retry policy](retry-policy.md) for full configuration and
behaviour details.

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

For the full design rationale, see
[design decisions — exception handling](design-decisions.md#exception-handling-in-runwithcontext).
