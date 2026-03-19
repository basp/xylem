namespace Xylem

open System
open System.Collections.Generic
open System.Threading.Tasks

module Domain =

    /// Produces a stream of records of type 'T.
    /// Calling Read () starts a fresh, independent stream each time.
    type Source<'T> = {
        Read: unit -> IAsyncEnumerable<'T>
    }

    /// Consumes a stream of records of type 'T.
    /// The sink owns iteration, allowing bulk operations and internal buffering.
    type Sink<'T> = {
        Write: IAsyncEnumerable<'T> -> Task<unit>
    }

    /// Transforms a stream of 'TIn records into a stream of 'TOut records.
    /// The transform is lazy — no work happens until the stream is consumed.
    type Flow<'TIn, 'TOut> = {
        Transform: IAsyncEnumerable<'TIn> -> IAsyncEnumerable<'TOut>
    }

    // -----------------------------------------------------------------------
    // Diagnostics
    // -----------------------------------------------------------------------

    /// How severe a diagnostic event is.
    type Severity =
        | Info      // Something noteworthy; pipeline is healthy
        | Warning   // Unexpected but recoverable; pipeline continues
        | Error     // A record could not be processed; it is rejected
        | Fatal     // Pipeline cannot continue; execution is aborted

    /// What went wrong — machine-readable and pattern-matchable.
    /// Use Custom for domain-specific kinds without modifying the library.
    /// Note: adding a new well-known case is a breaking change by design,
    /// forcing callers to explicitly handle it.
    type ErrorKind =
        | SystemError           of exn
        | IoError               of path: string * exn
        | ValidationError       of field: string * reason: string
        | BusinessRuleViolation of rule: string * reason: string
        | PipelineError         of stage: string * exn
        | Custom                of tag: string * data: Map<string, string>

    /// A single structured event emitted during a pipeline run.
    type DiagnosticEvent = {
        Severity:    Severity
        Kind:        ErrorKind
        /// The flow or stage that emitted this event, if applicable.
        Stage:       string option
        /// 0-based index of the record that triggered this event, if applicable.
        RecordIndex: int64 option
        Timestamp:   DateTimeOffset
        /// Human-readable summary of the event.
        Message:     string
    }

    /// The structured outcome of a completed pipeline run.
    type PipelineResult = {
        RecordsRead:     int64
        RecordsAccepted: int64
        RecordsRejected: int64
        RecordsFailed:   int64
        Duration:        TimeSpan
        Events:          DiagnosticEvent list
    }

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

    /// Builds a PipelineResult by folding over a list of diagnostic events.
    /// RecordsAccepted is derived as: read - rejected - failed.
    let fromEvents (recordsRead: int64) (duration: TimeSpan) (events: DiagnosticEvent list) =
        let rejected = events |> List.filter (fun e -> e.Severity = Error) |> List.length |> int64
        let failed   = events |> List.filter (fun e -> e.Severity = Fatal) |> List.length |> int64
        { RecordsRead     = recordsRead
          RecordsRejected = rejected
          RecordsFailed   = failed
          RecordsAccepted = recordsRead - rejected - failed
          Duration        = duration
          Events          = events }

module Flow =

    open Domain
    open FSharp.Control

    /// Creates a Flow that applies a mapping function to every item.
    let map (f: 'TIn -> 'TOut) : Flow<'TIn, 'TOut> = {
        Transform = TaskSeq.map f
    }

    /// Creates a Flow that keeps only items matching the predicate.
    let filter (predicate: 'T -> bool) : Flow<'T, 'T> = {
        Transform = TaskSeq.filter predicate
    }

    /// Composes two flows left-to-right: output of f1 becomes input of f2.
    let compose (f1: Flow<'T1, 'T2>) (f2: Flow<'T2, 'T3>) : Flow<'T1, 'T3> = {
        Transform = f1.Transform >> f2.Transform
    }

    /// Operator alias for compose — mirrors F# function composition style.
    let (>>>) f1 f2 = compose f1 f2

module Pipeline =

    open Domain

    /// Connects a Source directly to a Sink.
    let run (source: Source<'T>) (sink: Sink<'T>) : Task<unit> =
        sink.Write(source.Read())

    /// Connects a Source to a Sink, transforming records through a Flow.
    let runWith (source: Source<'TIn>) (flow: Flow<'TIn, 'TOut>) (sink: Sink<'TOut>) : Task<unit> =
        sink.Write(flow.Transform(source.Read()))

