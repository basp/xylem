module RootTests

open FSharp.Control
open Xunit
open Xylem.Domain

[<Fact>]
let ``Root Read returns all yielded items`` () = task {
    let root : Root<int> = {
        Read = fun () -> taskSeq { yield 1; yield 2; yield 3 }
    }

    let! items = root.Read() |> TaskSeq.toListAsync

    Assert.Equal<int list>([1; 2; 3], items)
}

[<Fact>]
let ``Root Read called twice produces independent streams`` () = task {
    let root : Root<int> = {
        Read = fun () -> taskSeq { yield 1; yield 2 }
    }

    let! first  = root.Read() |> TaskSeq.toListAsync
    let! second = root.Read() |> TaskSeq.toListAsync

    Assert.Equal<int list>(first, second)
    Assert.NotSame(first, second)
}

[<Fact>]
let ``Root Read can yield zero items`` () = task {
    let root : Root<int> = {
        Read = fun () -> TaskSeq.empty
    }

    let! items = root.Read() |> TaskSeq.toListAsync

    Assert.Empty(items)
}
