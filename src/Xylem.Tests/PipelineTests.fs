module PipelineTests

open System
open System.Threading
open FSharp.Control
open Xunit
open Xylem
open Xylem.Domain

// ---------------------------------------------------------------------------
// Pipeline.run
// ---------------------------------------------------------------------------

[<Fact>]
let ``Pipeline run passes source items to sink`` () = task {
    let source : Source<int> = {
        Read = fun () -> taskSeq { yield 10; yield 20; yield 30 }
    }
    let sink, read = Helpers.collectSink<int>()

    do! Pipeline.run source sink

    Assert.Equal<int list>([10; 20; 30], read ())
}

[<Fact>]
let ``Pipeline run with empty source results in empty sink`` () = task {
    let source : Source<int> = { Read = fun () -> TaskSeq.empty }
    let sink, read = Helpers.collectSink<int>()

    do! Pipeline.run source sink

    Assert.Empty(read ())
}

// ---------------------------------------------------------------------------
// Pipeline.runWithContext
// ---------------------------------------------------------------------------

[<Fact>]
let ``Pipeline runWithContext returns correct RecordsRead when all pass`` () = task {
    let ctx    = ExecutionContext.``default`` ()
    let source : Source<int> = { Read = fun () -> taskSeq { yield 1; yield 2; yield 3 } }
    let flow   = Flow.map id
    let sink, _ = Helpers.collectSink<int>()

    let! result = Pipeline.runWithContext ctx source flow sink

    Assert.Equal(3L, result.RecordsRead)
    Assert.Equal(3L, result.RecordsAccepted)
    Assert.Equal(0L, result.RecordsRejected)
    Assert.Equal(0L, result.RecordsFailed)
}

[<Fact>]
let ``Pipeline runWithContext captures validation rejections in PipelineResult`` () = task {
    let ctx    = ExecutionContext.``default`` ()
    let source : Source<int> = { Read = fun () -> taskSeq { yield -1; yield 2; yield -3; yield 4 } }
    let flow   = Flow.validate "stage" ctx Helpers.positiveValidator
    let sink, read = Helpers.collectSink<int>()

    let! result = Pipeline.runWithContext ctx source flow sink

    Assert.Equal(4L, result.RecordsRead)
    Assert.Equal(2L, result.RecordsRejected)
    Assert.Equal(2L, result.RecordsAccepted)
    Assert.Equal(0L, result.RecordsFailed)
    Assert.Equal<int list>([2; 4], read ())
}

[<Fact>]
let ``Pipeline runWithContext records a non-negative Duration`` () = task {
    let ctx    = ExecutionContext.``default`` ()
    let source : Source<int> = { Read = fun () -> TaskSeq.empty }
    let flow   = Flow.map id
    let sink, _ = Helpers.collectSink<int>()

    let! result = Pipeline.runWithContext ctx source flow sink

    Assert.True(result.Duration >= TimeSpan.Zero)
}

[<Fact>]
let ``Pipeline runWithContext with empty source returns zero counts`` () = task {
    let ctx    = ExecutionContext.``default`` ()
    let source : Source<int> = { Read = fun () -> TaskSeq.empty }
    let flow   = Flow.map id
    let sink, _ = Helpers.collectSink<int>()

    let! result = Pipeline.runWithContext ctx source flow sink

    Assert.Equal(0L, result.RecordsRead)
    Assert.Equal(0L, result.RecordsAccepted)
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

[<Fact>]
let ``Flow validate throws OperationCanceledException for already-cancelled token`` () = task {
    use cts  = new CancellationTokenSource()
    cts.Cancel()
    let ctx   = ExecutionContext.create cts.Token 1000
    let flow  = Flow.validate "stage" ctx Helpers.positiveValidator
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let mutable threw = false
    try
        let! _ = flow.Transform(input) |> TaskSeq.toListAsync
        ()
    with :? OperationCanceledException ->
        threw <- true

    Assert.True(threw)
}

[<Fact>]
let ``Pipeline runWithContext throws OperationCanceledException for already-cancelled token`` () = task {
    use cts  = new CancellationTokenSource()
    cts.Cancel()
    let ctx    = ExecutionContext.create cts.Token 1000
    let source : Source<int> = { Read = fun () -> taskSeq { yield 1; yield 2; yield 3 } }
    let flow   = Flow.map id
    let sink, _ = Helpers.collectSink<int>()

    let mutable threw = false
    try
        let! _ = Pipeline.runWithContext ctx source flow sink
        ()
    with :? OperationCanceledException ->
        threw <- true

    Assert.True(threw)
}
