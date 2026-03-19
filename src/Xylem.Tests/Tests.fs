module Tests

open Xunit
open FSharp.Control
open Xylem.Domain

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
}

[<Fact>]
let ``Source Read can yield zero items`` () = task {
    let source : Source<int> = {
        Read = fun () -> TaskSeq.empty
    }

    let! items = source.Read() |> TaskSeq.toListAsync

    Assert.Empty(items)
}
