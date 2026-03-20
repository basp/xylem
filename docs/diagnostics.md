# Diagnostics Model

Xylem's structured diagnostics — Severity, ErrorKind, Pulse, Harvest, and Ring — designed so failures are loud, structured, and traceable.

---

## Philosophy

Xylem's diagnostics are designed around one principle: **failures must be
loud, structured, and traceable.** A string error message is not enough —
you need to know *what* failed, *why*, *where* in the pipeline, and *which
record* was involved.

---

## `Severity`

Every diagnostic event carries a severity level:

```fsharp
type Severity =
    | Info      // Something noteworthy; pipeline is healthy
    | Warning   // Unexpected but recoverable; pipeline continues
    | Error     // A record could not be processed; it is rejected
    | Fatal     // Pipeline cannot continue; execution is aborted
```

---

## `ErrorKind`

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

`RetryError` is emitted by the retry engine (see [retry policy](retry-policy.md))
and carries the 1-based attempt number alongside the exception. Callers
can pattern-match on it to distinguish retry-related diagnostics from
other failures.

> **Adding a new well-known case is intentionally a breaking change.**
Exhaustive pattern matches will fail to compile, forcing every caller to
explicitly handle the new category. `Custom` is the safety valve when
you need a domain-specific kind without modifying the library.

---

## `Pulse`

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

---

## `Harvest`

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

`Events` is the source of truth for diagnostic events. The count fields
are pre-computed conveniences: `RecordsRejected` and `RecordsFailed` are
folded from event severities, while `RecordsAccepted` is computed as
`RecordsRead - RecordsRejected - RecordsFailed`. This keeps all fields
consistent and saves callers from folding the event list manually.

---

## `Ring`

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

### Producing summaries

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

### Typical usage

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

For design rationale behind Ring (standalone function vs field, grouping
strategy, ordering, count types), see
[design decisions — Ring](design-decisions.md#ring).
