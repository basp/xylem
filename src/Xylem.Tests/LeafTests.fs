module LeafTests

open FSharp.Control
open Xunit

[<Fact>]
let ``Leaf Write receives all items from stream`` () = task {
    let stream = taskSeq { yield "a"; yield "b"; yield "c" }
    let leaf, read = Helpers.collectSink<string>()

    do! leaf.Write(stream)

    Assert.Equal<string list>(["a"; "b"; "c"], read ())
}

[<Fact>]
let ``Leaf Write receives empty stream without error`` () = task {
    let leaf, read = Helpers.collectSink<int>()

    do! leaf.Write(TaskSeq.empty)

    Assert.Empty(read ())
}
