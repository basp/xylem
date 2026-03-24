# 🔄 Retry Policy

How Xylem retries failed Conduits — configuration, behaviour, and diagnostic events.

---

## `RetryPolicy`

```fsharp
type RetryPolicy =
    | NoRetry
    | FixedDelay of maxAttempts: int * delay: TimeSpan
```

| Case | Behaviour |
|---|---|
| `NoRetry` | Failure is immediately final. This is the default. |
| `FixedDelay(n, d)` | On failure, wait `d`, then re-run the Conduit from scratch. Repeat up to `n` times. Total executions = `n + 1` (initial + retries). |

---

## Configuring retry

Set `RetryPolicy` on the `ExecutionContext` using the `ExecutionContext.withRetryPolicy` helper or a record update:

```fsharp
// Using the helper (recommended)
let ctx =
    ExecutionContext.``default`` ()
    |> ExecutionContext.withRetryPolicy (FixedDelay(3, TimeSpan.FromSeconds 1.0))

// Using record update
let ctx =
    { ExecutionContext.``default`` () with
        RetryPolicy = FixedDelay(3, TimeSpan.FromSeconds 1.0) }
```

---

## What gets retried

The retry loop wraps the **entire Conduit**: root → vessel → leaf.
On each retry, `root.Read()` is called again, the vessel processes from
the beginning, and the sink receives a fresh stream. Both the record
count and diagnostic pulses are reset per attempt — `Harvest` reflects
only the latest (successful or final) attempt.

Note: Previous `RetryError` pulses are preserved so that the final
`Harvest` contains the full retry history, but transient pulses (like
`ValidationError` or `Fatal` exceptions from failed attempts) are cleared.

---

## Diagnostic events during retries

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
  Message  = "Conduit failed after 4 attempt(s): <message>" }
```

---

## Example: retry with inspection

This example configures a Conduit that retries up to twice on failure,
waiting 200ms between attempts. After the run, it prints the overall
counts and then extracts the retry-specific events from the result.
Because diagnostic events accumulate across all attempts, you get a
full history: a `Warning` for each failed-but-retried attempt, and — if
the Conduit never recovered — a final `Fatal` indicating exhaustion.

```fsharp
open System

let ctx =
    { ExecutionContext.``default`` () with
        RetryPolicy = FixedDelay(2, TimeSpan.FromMilliseconds 200.0) }

let! result = Conduit.runWithContext ctx root vessel leaf

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

## Cancellation during retries

If the `CancellationToken` is triggered during the delay between retries,
`OperationCanceledException` propagates immediately — the retry loop
does not swallow cancellation.

For the full design rationale (why whole-Conduit retry, why not
per-record, why `FixedDelay` only), see
[design decisions — retry policy](design-decisions.md#retry-policy).
