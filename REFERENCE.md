# Xylem Reference

This reference is derived from the XML doc comments in `src/Xylem/*.fs`.
It is intended as a quick lookup for the public API surface exposed by the
library.

---

## `Xylem.Domain`

### Pipeline core types

```fsharp
type Root<'T> = {
    Read: unit -> IAsyncEnumerable<'T>
}
```

The origin of a data pipeline. Each call to `Read ()` starts a fresh,
independent stream.

```fsharp
type Leaf<'T> = {
    Write: IAsyncEnumerable<'T> -> Task<unit>
}
```

The destination of a data pipeline. The leaf owns iteration, which allows
bulk operations and internal buffering.

```fsharp
type Vessel<'TIn, 'TOut> = {
    Transform: IAsyncEnumerable<'TIn> -> IAsyncEnumerable<'TOut>
}
```

A vessel carries records from `'TIn` to `'TOut`. The transform is lazy —
no work happens until the stream is consumed.

### Diagnostics

```fsharp
type Severity =
    | Info
    | Warning
    | Error
    | Fatal
```

Severity describes how serious a diagnostic event is.

```fsharp
type ErrorKind =
    | SystemError of exn
    | IoError of path: string * exn
    | ValidationError of field: string * reason: string
    | BusinessRuleViolation of rule: string * reason: string
    | PipelineError of stage: string * exn
    | Custom of tag: string * data: Map<string, string>
    | RetryError of attempt: int * exn
```

Machine-readable error kinds used by pulses and pipeline diagnostics.

```fsharp
type Pulse = {
    Severity: Severity
    Kind: ErrorKind
    Stage: string option
    RecordIndex: int64 option
    Timestamp: DateTimeOffset
    Message: string
}
```

A single structured pulse emitted during a pipeline run.

```fsharp
type RetryPolicy =
    | NoRetry
    | FixedDelay of maxAttempts: int * delay: TimeSpan
```

Controls how the pipeline retries on failure.

### Execution context

```fsharp
type ExecutionContext = {
    CancellationToken: CancellationToken
    BatchSize: int
    Emit: Pulse -> unit
    ReadEvents: unit -> Pulse list
    ClearEvents: unit -> unit
    RetryPolicy: RetryPolicy
}
```

Coordinates a single pipeline run: configuration, cancellation, and pulse
emission.

`ExecutionContext.create` creates a new context for one pipeline run.
`ExecutionContext.withRetryPolicy` returns a copy with a different retry
policy. `ExecutionContext.default` creates a context with
`CancellationToken.None` and a batch size of 1,000.

### Harvest and ring summaries

```fsharp
type Ring = {
    Stage: string option
    InfoCount: int64
    WarningCount: int64
    ErrorCount: int64
    FatalCount: int64
    TotalCount: int64
}
```

A tree ring — aggregated diagnostic counts for a single stage. Pulses with
no stage are grouped under `Stage = None`.

```fsharp
type Harvest = {
    RecordsRead: int64
    RecordsAccepted: int64
    RecordsRejected: int64
    RecordsFailed: int64
    Duration: TimeSpan
    Events: Pulse list
}
```

The structured outcome of a completed pipeline run.

`Harvest.fromEvents` folds a pulse list into a `Harvest`. The accepted
count is defined as `read - rejected - failed`.

`Harvest.summarizeByStage` aggregates pulses into rings, grouped by
`Stage`.

### Vessel helpers

`Xylem.Vessel` provides the standard transforms used by pipelines:

```fsharp
Vessel.map       : ('TIn -> 'TOut) -> Vessel<'TIn, 'TOut>
Vessel.filter    : ('T -> bool) -> Vessel<'T, 'T>
Vessel.compose   : Vessel<'T1, 'T2> -> Vessel<'T2, 'T3> -> Vessel<'T1, 'T3>
Vessel.(>>>)     : Vessel<'T1, 'T2> -> Vessel<'T2, 'T3> -> Vessel<'T1, 'T3>
Vessel.validate  : string -> ExecutionContext -> ('T -> Result<'T, ErrorKind>) -> Vessel<'T, 'T>
Vessel.enrich    : string -> ExecutionContext -> ('T -> Result<'TOut, ErrorKind>) -> Vessel<'T, 'TOut>
Vessel.batch     : int -> Vessel<'T, 'T[]>
Vessel.prune     : ('T -> bool) -> Vessel<'T, 'T>
Vessel.absorb    : string -> ExecutionContext -> ('T -> Result<'TOut, ErrorKind>) -> Vessel<'T, 'TOut>
Vessel.transmute : ('TIn -> 'TOut) -> Vessel<'TIn, 'TOut>
```

`validate` and `enrich` drop failing items and emit error pulses via the
provided execution context. `batch` groups consecutive items into arrays
of up to the configured size.

### Pipeline helpers

```fsharp
Pipeline.run           : Root<'T> -> Leaf<'T> -> Task<unit>
Pipeline.runWith       : Root<'TIn> -> Vessel<'TIn, 'TOut> -> Leaf<'TOut> -> Task<unit>
Pipeline.runWithContext: ExecutionContext -> Root<'TIn> -> Vessel<'TIn, 'TOut> -> Leaf<'TOut> -> Task<Harvest>
Pipeline.flow          : ExecutionContext -> Root<'TIn> -> Vessel<'TIn, 'TOut> -> Leaf<'TOut> -> Task<Harvest>
```

`runWithContext` catches unhandled exceptions, records a `Fatal` pulse,
and returns a well-formed `Harvest` rather than faulting the task. When a
retry policy is configured, the pipeline is re-executed from scratch on
failure.

---

## `Xylem.Connectors.InMemory`

In-memory connectors for tests, examples, and simple one-off pipelines.
They have no I/O and no external dependencies.

```fsharp
InMemory.source : #seq<'T> -> Root<'T>
```

Creates a root that produces items from the provided sequence. Each call
to `Read ()` starts a fresh stream over the same sequence.

```fsharp
InMemory.sink : unit -> Leaf<'T> * (unit -> 'T list)
```

Creates an in-memory leaf that accumulates every written record into an
internal buffer and returns a reader function that snapshots the collected
items as a list.

---

## `Xylem.Connectors.File`

The file connector is line-oriented: it produces and consumes `string`
lines. Parsing and serialisation belong in a `Vessel`.

```fsharp
type FileLeafOptions = {
    Append: bool
    Encoding: Encoding
}
```

Options controlling how `File.sink` writes to a file. `FileLeafOptions.Default`
uses overwrite mode and UTF-8 without BOM.

```fsharp
File.sourceFrom  : (unit -> TextReader) -> Root<string>
File.source      : string -> Root<string>
File.sinkFrom    : (unit -> TextWriter) -> Leaf<string>
File.sink        : string -> FileLeafOptions -> Leaf<string>
File.sinkDefault : string -> Leaf<string>
```

`sourceFrom` and `sinkFrom` are the primary testable constructors.
`source` and `sink` are thin wrappers that open a `StreamReader` or
`StreamWriter` for a path.

Resource lifetime is owned by the connector. The reader or writer produced
by the factory is disposed when the operation finishes.

---

## `Xylem.Connectors.Json`

The JSON connector reads and writes records as a single JSON array at the
root of the stream or file.

```fsharp
type JsonLeafOptions = {
    WriteIndented: bool
    Encoding: Encoding
}
```

Options controlling how `Json.sink` writes to a file.

```fsharp
Json.sourceFrom     : (unit -> Stream) -> Root<'T>
Json.source         : string -> Root<'T>
Json.sinkFrom       : (unit -> Stream) -> JsonLeafOptions -> Leaf<'T>
Json.sinkFromDefault: (unit -> Stream) -> Leaf<'T>
Json.sink           : string -> JsonLeafOptions -> Leaf<'T>
Json.sinkDefault    : string -> Leaf<'T>
```

`sourceFrom` deserializes a JSON array from a stream factory and disposes
the stream when enumeration ends. `sinkFrom` serializes items as a JSON
array and disposes the stream after writing. The `source` and `sink`
overloads are file-based wrappers.

`Json.source` expects a single JSON array at the root of the input. It
does not support JSON Lines (JSONL) or multiple loose JSON objects.

For test code, inject a `MemoryStream` factory. If you need to inspect the
written output after `sinkFrom` returns, use `MemoryStream.ToArray()`
rather than `Position` or `Read`, because the stream is disposed by the
connector.
