namespace Xylem

open System
open System.Collections.Generic
open System.Threading.Tasks

module Domain =

    /// <summary>
    /// Produces a stream of records of type <c>'T</c>.
    /// Calling <c>Read ()</c> starts a fresh, independent stream each time.
    /// </summary>
    type Source<'T> = {
        /// <summary>
        /// Starts a fresh, independent stream of records of type <c>'T</c>.
        /// </summary>
        Read: unit -> IAsyncEnumerable<'T>
    }

    /// <summary>
    /// Consumes a stream of records of type <c>'T</c>.
    /// The sink owns iteration, allowing bulk operations and internal buffering.
    /// </summary>
    type Sink<'T> = {
        /// <summary>
        /// Consumes a stream of records of type <c>'T</c> and performs the sink operation.
        /// </summary>
        Write: IAsyncEnumerable<'T> -> Task<unit>
    }

    /// <summary>
    /// Transforms a stream of <c>'TIn</c> records into a stream of <c>'TOut</c> records.
    /// The transform is lazy — no work happens until the stream is consumed.
    /// </summary>
    type Flow<'TIn, 'TOut> = {
        /// <summary>
        /// Applies the underlying transform from <c>'TIn</c> to <c>'TOut</c>.
        /// </summary>
        Transform: IAsyncEnumerable<'TIn> -> IAsyncEnumerable<'TOut>
    }

    // -----------------------------------------------------------------------
    // Diagnostics
    // -----------------------------------------------------------------------

    /// <summary>
    /// How severe a diagnostic event is.
    /// </summary>
    type Severity =
        /// <summary>
        /// Something noteworthy; the pipeline is healthy.
        /// </summary>
        | Info
        /// <summary>
        /// Unexpected but recoverable; the pipeline continues.
        /// </summary>
        | Warning
        /// <summary>
        /// A record could not be processed and is rejected.
        /// </summary>
        | Error
        /// <summary>
        /// Pipeline cannot continue; execution is aborted.
        /// </summary>
        | Fatal

    /// <summary>
    /// What went wrong — machine-readable and pattern-matchable.
    /// Use <c>Custom</c> for domain-specific kinds without modifying the library.
    /// Note: adding a new well-known case is a breaking change by design,
    /// forcing callers to explicitly handle it.
    /// </summary>
    type ErrorKind =
        /// <summary>
        /// System-level error (e.g., exception).
        /// </summary>
        | SystemError           of exn
        /// <summary>
        /// IO error with the provided path and inner exception.
        /// </summary>
        | IoError               of path: string * exn
        /// <summary>
        /// Validation error for a specific field and reason.
        /// </summary>
        | ValidationError       of field: string * reason: string
        /// <summary>
        /// Business rule violation with rule and reason.
        /// </summary>
        | BusinessRuleViolation of rule: string * reason: string
        /// <summary>
        /// Pipeline error associated with a stage and exception.
        /// </summary>
        | PipelineError         of stage: string * exn
        /// <summary>
        /// Custom error with tag and data payload.
        /// </summary>
        | Custom                of tag: string * data: Map<string, string>
        /// <summary>
        /// A retry attempt failed. Carries the 1-based attempt number and the exception.
        /// </summary>
        | RetryError            of attempt: int * exn

    /// <summary>
    /// A single structured event emitted during a pipeline run.
    /// </summary>
    type DiagnosticEvent = {
        /// <summary>
        /// How severe the event is.
        /// </summary>
        Severity:    Severity
        /// <summary>
        /// What kind of error or event occurred.
        /// </summary>
        Kind:        ErrorKind
        /// <summary>
        /// The flow or stage that emitted this event, if applicable.
        /// </summary>
        Stage:       string option
        /// <summary>
        /// 0-based index of the record that triggered this event, if applicable.
        /// </summary>
        RecordIndex: int64 option
        /// <summary>
        /// When the event occurred.
        /// </summary>
        Timestamp:   DateTimeOffset
        /// <summary>
        /// Human-readable summary of the event.
        /// </summary>
        Message:     string
    }

    /// <summary>
    /// Controls how the pipeline retries on failure.
    /// <c>NoRetry</c> means failures are immediately final.
    /// <c>FixedDelay</c> retries up to <c>maxAttempts</c> times with a constant delay between attempts.
    /// </summary>
    type RetryPolicy =
        /// <summary>No retries — a failure is immediately final.</summary>
        | NoRetry
        /// <summary>
        /// Retry up to <c>maxAttempts</c> times with a constant <c>delay</c> between attempts.
        /// The total number of executions is <c>maxAttempts + 1</c> (initial + retries).
        /// </summary>
        | FixedDelay of maxAttempts: int * delay: TimeSpan

    /// <summary>
    /// Coordinates a single pipeline run: configuration, cancellation, and diagnostic emission.
    /// Create via <c>ExecutionContext.create</c> or <c>ExecutionContext.default</c>.
    /// </summary>
    type ExecutionContext = {
        /// <summary>Signals cooperative cancellation to the pipeline.</summary>
        CancellationToken: System.Threading.CancellationToken
        /// <summary>Preferred number of records per batch for batch-aware sinks and flows.</summary>
        BatchSize:         int
        /// <summary>
        /// Emits a structured diagnostic event for the current run.
        /// Called by flows that reject or warn about individual records.
        /// </summary>
        Emit:              DiagnosticEvent -> unit
        /// <summary>Returns all events emitted so far in this run, in emission order.</summary>
        ReadEvents:        unit -> DiagnosticEvent list
        /// <summary>Retry policy for the pipeline run. Defaults to <c>NoRetry</c>.</summary>
        RetryPolicy:       RetryPolicy
    }

    /// <summary>
    /// Aggregated diagnostic counts for a single pipeline stage.
    /// Events with no <c>Stage</c> are grouped under <c>Stage = None</c>.
    /// </summary>
    type StageSummary = {
        /// <summary>The stage name, or <c>None</c> for pipeline-level events.</summary>
        Stage:        string option
        /// <summary>Number of <c>Info</c>-level events in this stage.</summary>
        InfoCount:    int64
        /// <summary>Number of <c>Warning</c>-level events in this stage.</summary>
        WarningCount: int64
        /// <summary>Number of <c>Error</c>-level events in this stage.</summary>
        ErrorCount:   int64
        /// <summary>Number of <c>Fatal</c>-level events in this stage.</summary>
        FatalCount:   int64
        /// <summary>Total number of events in this stage.</summary>
        TotalCount:   int64
    }

    /// <summary>
    /// The structured outcome of a completed pipeline run.
    /// </summary>
    type PipelineResult = {
        /// <summary>
        /// Number of records read from the source.
        /// </summary>
        RecordsRead:     int64
        /// <summary>
        /// Number of records accepted by the pipeline.
        /// </summary>
        RecordsAccepted: int64
        /// <summary>
        /// Number of records rejected by the pipeline.
        /// </summary>
        RecordsRejected: int64
        /// <summary>
        /// Number of records that failed fatally.
        /// </summary>
        RecordsFailed:   int64
        /// <summary>
        /// Total duration of the pipeline run.
        /// </summary>
        Duration:        TimeSpan
        /// <summary>
        /// List of diagnostic events emitted during the run.
        /// </summary>
        Events:          DiagnosticEvent list
    }

module ExecutionContext =

    open Domain

    /// <summary>
    /// Creates a new <c>ExecutionContext</c> for a single pipeline run.
    /// All events emitted via <c>Emit</c> are readable via <c>ReadEvents</c>.
    /// <c>Emit</c> is thread-safe: concurrent calls are serialized via a lock,
    /// and the emission order is preserved.
    /// </summary>
    let create (token: System.Threading.CancellationToken) (batchSize: int) : ExecutionContext =
        let events = ResizeArray<DiagnosticEvent>()
        let gate   = obj ()
        { CancellationToken = token
          BatchSize         = batchSize
          Emit              = fun e -> lock gate (fun () -> events.Add(e))
          ReadEvents        = fun () -> lock gate (fun () -> List.ofSeq events)
          RetryPolicy       = NoRetry }

    /// <summary>
    /// Creates an <c>ExecutionContext</c> with <c>CancellationToken.None</c> and
    /// a batch size of 1 000 — suitable for tests and simple one-off runs.
    /// </summary>
    let ``default`` () =
        create System.Threading.CancellationToken.None 1_000

module PipelineResult =

    open Domain

    let empty = {
        RecordsRead     = 0L
        RecordsAccepted = 0L
        RecordsRejected = 0L
        RecordsFailed   = 0L
        Duration        = TimeSpan.Zero
        Events          = []
    }

    /// <summary>
    /// Builds a PipelineResult by folding over a list of diagnostic events.
    /// The value of RecordsAccepted is defined as: read - rejected - failed.
    /// </summary>
    let fromEvents (recordsRead: int64) (duration: TimeSpan) (events: DiagnosticEvent list) =
        let rejected = events |> List.filter (fun e -> e.Severity = Error) |> List.length |> int64
        let failed   = events |> List.filter (fun e -> e.Severity = Fatal) |> List.length |> int64
        { RecordsRead     = recordsRead
          RecordsRejected = rejected
          RecordsFailed   = failed
          RecordsAccepted = recordsRead - rejected - failed
          Duration        = duration
          Events          = events }

    /// <summary>
    /// Aggregates a list of diagnostic events into per-stage summaries.
    /// Events are grouped by <c>Stage</c> (with <c>None</c> as a valid group
    /// for pipeline-level events). The order of summaries follows the first
    /// occurrence of each stage in the event list.
    /// </summary>
    let summarizeByStage (events: DiagnosticEvent list) : StageSummary list =
        let order = ResizeArray<string option>()
        let acc   = Dictionary<string, int64 * int64 * int64 * int64>()
        let sentinel = "\x00__none__"

        let toKey (stage: string option) = stage |> Option.defaultValue sentinel

        for e in events do
            let key = toKey e.Stage
            if not (acc.ContainsKey key) then
                order.Add e.Stage
                acc[key] <- (0L, 0L, 0L, 0L)
            let i, w, er, f = acc[key]
            acc[key] <-
                match e.Severity with
                | Info    -> (i + 1L, w, er, f)
                | Warning -> (i, w + 1L, er, f)
                | Error   -> (i, w, er + 1L, f)
                | Fatal   -> (i, w, er, f + 1L)

        [ for stage in order do
            let i, w, er, f = acc[toKey stage]
            { Stage        = stage
              InfoCount    = i
              WarningCount = w
              ErrorCount   = er
              FatalCount   = f
              TotalCount   = i + w + er + f } ]

module Flow =

    open Domain
    open FSharp.Control

    /// <summary>
    /// Creates a <c>Flow</c> that applies a mapping function to every item.
    /// </summary>
    let map (f: 'TIn -> 'TOut) : Flow<'TIn, 'TOut> = {
        Transform = TaskSeq.map f
    }

    /// <summary>
    /// Creates a <c>Flow</c> that keeps only items matching the predicate.
    /// </summary>
    let filter (predicate: 'T -> bool) : Flow<'T, 'T> = {
        Transform = TaskSeq.filter predicate
    }

    /// <summary>
    /// Composes two flows left-to-right: output of <c>f1</c> becomes input of <c>f2</c>.
    /// </summary>
    let compose (f1: Flow<'T1, 'T2>) (f2: Flow<'T2, 'T3>) : Flow<'T1, 'T3> = {
        Transform = f1.Transform >> f2.Transform
    }

    /// <summary>
    /// Operator alias for <c>compose</c> — mirrors F# function composition style.
    /// </summary>
    let (>>>) f1 f2 = compose f1 f2

    /// <summary>
    /// Creates a <c>Flow</c> that validates every item using <c>validator</c>.
    /// Items for which <c>validator</c> returns <c>Ok</c> are passed downstream unchanged.
    /// Items for which it returns <c>Error</c> are dropped, and a <c>DiagnosticEvent</c>
    /// of severity <c>Error</c> is emitted via <c>ctx</c>, recording the stage name,
    /// 0-based stream index, and the <c>ErrorKind</c> returned by the validator.
    /// </summary>
    let validate (stageName: string) (ctx: ExecutionContext) (validator: 'T -> Result<'T, ErrorKind>) : Flow<'T, 'T> =
        { Transform = fun stream ->
            // Mutable `index` works because `taskSeq` iterates sequentially.
            let mutable index = 0L
            taskSeq {
                for item in stream do
                    ctx.CancellationToken.ThrowIfCancellationRequested()
                    match validator item with
                    | Result.Ok accepted ->
                        index <- index + 1L
                        yield accepted
                    | Result.Error kind ->
                        ctx.Emit {
                            Severity    = Severity.Error
                            Kind        = kind
                            Stage       = Some stageName
                            RecordIndex = Some index
                            Timestamp   = DateTimeOffset.UtcNow
                            Message     = $"Record at index {index} rejected by '{stageName}'"
                        }
                        index <- index + 1L
            } }

    /// <summary>
    /// Creates a <c>Flow</c> that enriches every item using <c>enricher</c>.
    /// Items for which <c>enricher</c> returns <c>Ok</c> are passed downstream
    /// as the (potentially type-changed) enriched value.
    /// Items for which it returns <c>Error</c> are dropped, and a
    /// <c>DiagnosticEvent</c> of severity <c>Error</c> is emitted via <c>ctx</c>,
    /// recording the stage name, 0-based stream index, and the <c>ErrorKind</c>
    /// returned by the enricher.
    /// Unlike <c>validate</c>, the enricher may change the record type from
    /// <c>'T</c> to <c>'TOut</c> — useful for lookups, projections, and joins.
    /// </summary>
    let enrich (stageName: string) (ctx: ExecutionContext) (enricher: 'T -> Result<'TOut, ErrorKind>) : Flow<'T, 'TOut> =
        { Transform = fun stream ->
            let mutable index = 0L
            taskSeq {
                for item in stream do
                    ctx.CancellationToken.ThrowIfCancellationRequested()
                    match enricher item with
                    | Result.Ok enriched ->
                        index <- index + 1L
                        yield enriched
                    | Result.Error kind ->
                        ctx.Emit {
                            Severity    = Severity.Error
                            Kind        = kind
                            Stage       = Some stageName
                            RecordIndex = Some index
                            Timestamp   = DateTimeOffset.UtcNow
                            Message     = $"Record at index {index} could not be enriched by '{stageName}'"
                        }
                        index <- index + 1L
            } }

    /// <summary>
    /// Creates a <c>Flow</c> that groups consecutive items into arrays of at most
    /// <c>batchSize</c> items. The final batch is emitted even if it contains
    /// fewer than <c>batchSize</c> items.
    /// Throws <c>ArgumentException</c> if <c>batchSize</c> is less than 1.
    /// To use the pipeline's configured batch size, pass <c>ctx.BatchSize</c>.
    /// </summary>
    let batch (batchSize: int) : Flow<'T, 'T[]> =
        if batchSize < 1 then
            raise (ArgumentException($"batchSize must be >= 1, was {batchSize}", nameof batchSize))
        { Transform = fun stream ->
            taskSeq {
                let buffer = ResizeArray<'T>(batchSize)
                for item in stream do
                    buffer.Add(item)
                    if buffer.Count = batchSize then
                        yield buffer.ToArray()
                        buffer.Clear()
                if buffer.Count > 0 then
                    yield buffer.ToArray()
            } }

module Pipeline =

    open Domain
    open FSharp.Control

    /// <summary>
    /// Connects a <c>Source</c> directly to a <c>Sink</c>.
    /// </summary>
    let run (source: Source<'T>) (sink: Sink<'T>) : Task<unit> =
        sink.Write(source.Read())

    /// <summary>
    /// Connects a <c>Source</c> to a <c>Sink</c>, transforming records through a <c>Flow</c>.
    /// </summary>
    let runWith (source: Source<'TIn>) (flow: Flow<'TIn, 'TOut>) (sink: Sink<'TOut>) : Task<unit> =
        sink.Write(flow.Transform(source.Read()))

    /// <summary>
    /// Runs a pipeline under an <c>ExecutionContext</c> and returns a structured
    /// <c>PipelineResult</c>. Counts every record emitted by the source, measures
    /// wall-clock duration, and collects all diagnostic events from <c>ctx</c>.
    /// Any unhandled exception (e.g., a connector I/O failure) is caught, emitted
    /// as a <c>Fatal</c> diagnostic event, and the function returns a well-formed
    /// <c>PipelineResult</c> reflecting the partial run rather than faulting the task.
    /// When <c>ctx.RetryPolicy</c> is not <c>NoRetry</c>, the entire pipeline is
    /// re-executed from scratch on failure, up to the configured number of attempts.
    /// Each retry emits a <c>Warning</c>-level diagnostic event. If all retries are
    /// exhausted, a <c>Fatal</c> event is emitted and the partial result is returned.
    /// </summary>
    let runWithContext
            (ctx:    ExecutionContext)
            (source: Source<'TIn>)
            (flow:   Flow<'TIn, 'TOut>)
            (sink:   Sink<'TOut>)
            : Task<PipelineResult> =
        task {
            ctx.CancellationToken.ThrowIfCancellationRequested()
            let sw    = System.Diagnostics.Stopwatch.StartNew()

            let maxAttempts, delay =
                match ctx.RetryPolicy with
                | NoRetry                       -> 0, TimeSpan.Zero
                | FixedDelay (maxAttempts, dly) -> maxAttempts, dly

            let mutable attempt   = 0
            let mutable succeeded = false
            let count             = ref 0L

            while not succeeded && attempt <= maxAttempts do
                count.Value <- 0L

                let countingSource : Source<'TIn> = {
                    Read = fun () ->
                        source.Read()
                        |> TaskSeq.map (fun item ->
                            count.Value <- count.Value + 1L
                            item)
                }

                try
                    do! runWith countingSource flow sink
                    succeeded <- true
                with ex ->
                    if attempt < maxAttempts then
                        ctx.Emit {
                            Severity    = Severity.Warning
                            Kind        = ErrorKind.RetryError (attempt + 1, ex)
                            Stage       = None
                            RecordIndex = None
                            Timestamp   = DateTimeOffset.UtcNow
                            Message     = $"Attempt {attempt + 1} failed: {ex.Message}. Retrying in {delay.TotalMilliseconds}ms…"
                        }
                        do! Task.Delay(delay, ctx.CancellationToken)
                    else
                        ctx.Emit {
                            Severity    = Severity.Fatal
                            Kind        = ErrorKind.RetryError (attempt + 1, ex)
                            Stage       = None
                            RecordIndex = None
                            Timestamp   = DateTimeOffset.UtcNow
                            Message     = $"Pipeline failed after {attempt + 1} attempt(s): {ex.Message}"
                        }

                    attempt <- attempt + 1

            sw.Stop()
            return PipelineResult.fromEvents count.Value sw.Elapsed (ctx.ReadEvents())
        }
