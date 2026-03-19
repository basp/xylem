module Tests

open System.Collections.Generic
open Xunit
open FSharp.Control
open Xylem
open Xylem.Domain
open Xylem.Flow

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// In-memory sink that accumulates items into a List for assertion.
let collectSink<'T> () =
    let collected = List<'T>()
    let sink : Sink<'T> = {
        Write = fun stream -> task {
            do! stream |> TaskSeq.iter collected.Add
        }
    }
    sink, collected

// ---------------------------------------------------------------------------
// Source<'T>
// ---------------------------------------------------------------------------

[<Fact>]
let ``Source Read returns all yielded items`` () = task {
    let source : Source<int> = {
        Read = fun () -> taskSeq { yield 1; yield 2; yield 3 }
    }

    let! items = source.Read() |> TaskSeq.toListAsync

    Assert.Equal<int list>([1; 2; 3], items)
}

[<Fact>]
let ``Source Read called twice produces independent streams`` () = task {
    let source : Source<int> = {
        Read = fun () -> taskSeq { yield 1; yield 2 }
    }

    let! first  = source.Read() |> TaskSeq.toListAsync
    let! second = source.Read() |> TaskSeq.toListAsync

    Assert.Equal<int list>(first, second)
    Assert.NotSame(first, second)
}

[<Fact>]
let ``Source Read can yield zero items`` () = task {
    let source : Source<int> = {
        Read = fun () -> TaskSeq.empty
    }

    let! items = source.Read() |> TaskSeq.toListAsync

    Assert.Empty(items)
}

// ---------------------------------------------------------------------------
// Sink<'T>
// ---------------------------------------------------------------------------

[<Fact>]
let ``Sink Write receives all items from stream`` () = task {
    let stream = taskSeq { yield "a"; yield "b"; yield "c" }
    let sink, collected = collectSink<string>()

    do! sink.Write(stream)

    Assert.Equal<string list>(["a"; "b"; "c"], List.ofSeq collected)
}

[<Fact>]
let ``Sink Write receives empty stream without error`` () = task {
    let sink, collected = collectSink<int>()

    do! sink.Write(TaskSeq.empty)

    Assert.Empty(collected)
}

// ---------------------------------------------------------------------------
// Pipeline.run — source -> sink
// ---------------------------------------------------------------------------

[<Fact>]
let ``Pipeline run passes source items to sink`` () = task {
    let source : Source<int> = {
        Read = fun () -> taskSeq { yield 10; yield 20; yield 30 }
    }
    let sink, collected = collectSink<int>()

    do! Pipeline.run source sink

    Assert.Equal<int list>([10; 20; 30], List.ofSeq collected)
}

[<Fact>]
let ``Pipeline run with empty source results in empty sink`` () = task {
    let source : Source<int> = { Read = fun () -> TaskSeq.empty }
    let sink, collected = collectSink<int>()

    do! Pipeline.run source sink

    Assert.Empty(collected)
}

// ---------------------------------------------------------------------------
// Flow<'TIn,'TOut>
// ---------------------------------------------------------------------------

[<Fact>]
let ``Flow map transforms every item`` () = task {
    let flow = Flow.map (fun x -> x * 2)
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([2; 4; 6], result)
}

[<Fact>]
let ``Flow filter keeps only matching items`` () = task {
    let flow = Flow.filter (fun x -> x % 2 = 0)
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([2; 4], result)
}

[<Fact>]
let ``Flow map then filter via compose`` () = task {
    let flow = Flow.map (fun x -> x * 2) >>> Flow.filter (fun x -> x > 4)
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([6], result)
}

[<Fact>]
let ``Flow map on empty stream yields empty stream`` () = task {
    let flow = Flow.map (fun x -> x * 2)

    let! result = flow.Transform(TaskSeq.empty) |> TaskSeq.toListAsync

    Assert.Empty(result)
}

[<Fact>]
let ``Pipeline runWith threads source through flow into sink`` () = task {
    let source : Source<int> = {
        Read = fun () -> taskSeq { yield 1; yield 2; yield 3; yield 4; yield 5 }
    }
    let flow   = Flow.filter (fun x -> x % 2 <> 0)
    let sink, collected = collectSink<int>()

    do! Pipeline.runWith source flow sink

    Assert.Equal<int list>([1; 3; 5], List.ofSeq collected)
}
