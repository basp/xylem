module SinkTests

open FSharp.Control
open Xunit

[<Fact>]
let ``Sink Write receives all items from stream`` () = task {
    let stream = taskSeq { yield "a"; yield "b"; yield "c" }
    let sink, read = Helpers.collectSink<string>()

    do! sink.Write(stream)

    Assert.Equal<string list>(["a"; "b"; "c"], read ())
}

[<Fact>]
let ``Sink Write receives empty stream without error`` () = task {
    let sink, read = Helpers.collectSink<int>()

    do! sink.Write(TaskSeq.empty)

    Assert.Empty(read ())
}
