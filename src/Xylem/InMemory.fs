namespace Xylem.Connectors

open FSharp.Control
open Xylem.Domain

/// <summary>
/// In-memory connectors for tests, examples, and simple one-off pipelines.
/// No I/O, no external dependencies.
/// </summary>
module InMemory =

    /// <summary>
    /// Creates a <c>Source&lt;'T&gt;</c> that produces items from <c>items</c>.
    /// Each call to <c>Read ()</c> starts a fresh, independent stream over the
    /// same sequence. If <c>items</c> is a one-shot <c>seq</c> that can only be
    /// iterated once, only the first <c>Read ()</c> call will return data.
    /// </summary>
    let source (items: #seq<'T>) : Source<'T> = {
        Read = fun () -> TaskSeq.ofSeq items
    }

    /// <summary>
    /// Creates an in-memory <c>Sink&lt;'T&gt;</c> that accumulates every written
    /// record into an internal buffer.
    /// Returns the sink and a reader function. Each call to the reader returns
    /// a fresh <c>'T list</c> snapshot of the items collected so far —
    /// consistent with the pattern used by <c>ExecutionContext.ReadEvents</c>.
    /// </summary>
    let sink<'T> () : Sink<'T> * (unit -> 'T list) =
        let buffer = ResizeArray<'T>()
        let sink : Sink<'T> = {
            Write = fun stream -> task {
                do! stream |> TaskSeq.iter buffer.Add
            }
        }
        sink, fun () -> List.ofSeq buffer
