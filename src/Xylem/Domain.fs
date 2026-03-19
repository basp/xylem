namespace Xylem

open System.Collections.Generic

module Domain =

    /// Produces a stream of records of type 'T.
    /// Calling Read () starts a fresh, independent stream each time.
    type Source<'T> = {
        Read: unit -> IAsyncEnumerable<'T>
    }
