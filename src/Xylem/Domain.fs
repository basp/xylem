namespace Xylem

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
