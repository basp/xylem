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

module Pipeline =

    open Domain

    /// Connects a Source to a Sink, passing the source stream directly to the sink.
    let run (source: Source<'T>) (sink: Sink<'T>) : Task<unit> =
        sink.Write(source.Read())
