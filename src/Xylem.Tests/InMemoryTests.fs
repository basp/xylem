module InMemoryTests

open FSharp.Control
open Xunit
open Xylem
open Xylem.Connectors

[<Fact>]
let ``InMemory source yields all items from a list`` () = task {
    let source = InMemory.source [1; 2; 3]

    let! items = source.Read() |> TaskSeq.toListAsync

    Assert.Equal<int list>([1; 2; 3], items)
}

[<Fact>]
let ``InMemory source yields all items from an array`` () = task {
    let source = InMemory.source [| "a"; "b"; "c" |]

    let! items = source.Read() |> TaskSeq.toListAsync

    Assert.Equal<string list>(["a"; "b"; "c"], items)
}

[<Fact>]
let ``InMemory source from empty sequence yields empty stream`` () = task {
    let source = InMemory.source ([] : int list)

    let! items = source.Read() |> TaskSeq.toListAsync

    Assert.Empty(items)
}

[<Fact>]
let ``InMemory source Read called twice produces independent streams`` () = task {
    let source = InMemory.source [1; 2; 3]

    let! first  = source.Read() |> TaskSeq.toListAsync
    let! second = source.Read() |> TaskSeq.toListAsync

    Assert.Equal<int list>(first, second)
    Assert.NotSame(first, second)
}

[<Fact>]
let ``InMemory sink collects all written items`` () = task {
    let sink, read = InMemory.sink ()
    let stream = taskSeq { yield 1; yield 2; yield 3 }

    do! sink.Write(stream)

    Assert.Equal<int list>([1; 2; 3], read ())
}

[<Fact>]
let ``InMemory sink snapshot is empty before write`` () =
    let _, read = InMemory.sink<int> ()

    Assert.Empty(read ())

[<Fact>]
let ``InMemory sink write with empty stream yields empty snapshot`` () = task {
    let sink, read = InMemory.sink<int> ()

    do! sink.Write(TaskSeq.empty)

    Assert.Empty(read ())
}

[<Fact>]
let ``InMemory sink read returns a fresh snapshot each call`` () = task {
    let sink, read = InMemory.sink ()
    let stream = taskSeq { yield "x"; yield "y" }

    do! sink.Write(stream)
    let snap1 = read ()
    let snap2 = read ()

    Assert.Equal<string list>(snap1, snap2)
    Assert.NotSame(snap1, snap2)
}

[<Fact>]
let ``InMemory source and sink round-trip a full pipeline`` () = task {
    let source     = InMemory.source [1; 2; 3; 4; 5]
    let flow       = Flow.filter (fun x -> x % 2 = 0)
    let sink, read = InMemory.sink ()

    do! Pipeline.runWith source flow sink

    Assert.Equal<int list>([2; 4], read ())
}
