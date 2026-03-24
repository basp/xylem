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
let ``Pipeline run passes root items to leaf`` () = task {
    let root : Root<int> = {
        Read = fun () -> taskSeq { yield 10; yield 20; yield 30 }
    }
    let leaf, read = Helpers.collectSink<int>()

    do! Pipeline.run root leaf

    Assert.Equal<int list>([10; 20; 30], read ())
}

[<Fact>]
let ``Pipeline run with empty root results in empty leaf`` () = task {
    let root : Root<int> = { Read = fun () -> TaskSeq.empty }
    let leaf, read = Helpers.collectSink<int>()

    do! Pipeline.run root leaf

    Assert.Empty(read ())
}

// ---------------------------------------------------------------------------
// Pipeline.runWithContext
// ---------------------------------------------------------------------------

[<Fact>]
let ``Pipeline runWithContext returns correct RecordsRead when all pass`` () = task {
    let ctx    = ExecutionContext.``default`` ()
    let root : Root<int> = { Read = fun () -> taskSeq { yield 1; yield 2; yield 3 } }
    let vessel = Vessel.map id
    let leaf, _ = Helpers.collectSink<int>()

    let! result = Pipeline.runWithContext ctx root vessel leaf

    Assert.Equal(3L, result.RecordsRead)
    Assert.Equal(3L, result.RecordsAccepted)
    Assert.Equal(0L, result.RecordsRejected)
    Assert.Equal(0L, result.RecordsFailed)
}

[<Fact>]
let ``Pipeline runWithContext captures validation rejections in PipelineResult`` () = task {
    let ctx    = ExecutionContext.``default`` ()
    let root : Root<int> = { Read = fun () -> taskSeq { yield -1; yield 2; yield -3; yield 4 } }
    let vessel = Vessel.validate "stage" ctx Helpers.positiveValidator
    let leaf, read = Helpers.collectSink<int>()

    let! result = Pipeline.runWithContext ctx root vessel leaf

    Assert.Equal(4L, result.RecordsRead)
    Assert.Equal(2L, result.RecordsRejected)
    Assert.Equal(2L, result.RecordsAccepted)
    Assert.Equal(0L, result.RecordsFailed)
    Assert.Equal<int list>([2; 4], read ())
}

[<Fact>]
let ``Pipeline runWithContext records a non-negative Duration`` () = task {
    let ctx    = ExecutionContext.``default`` ()
    let root : Root<int> = { Read = fun () -> TaskSeq.empty }
    let vessel = Vessel.map id
    let leaf, _ = Helpers.collectSink<int>()

    let! result = Pipeline.runWithContext ctx root vessel leaf

    Assert.True(result.Duration >= TimeSpan.Zero)
}

[<Fact>]
let ``Pipeline runWithContext with empty source returns zero counts`` () = task {
    let ctx    = ExecutionContext.``default`` ()
    let root : Root<int> = { Read = fun () -> TaskSeq.empty }
    let vessel = Vessel.map id
    let leaf, _ = Helpers.collectSink<int>()

    let! result = Pipeline.runWithContext ctx root vessel leaf

    Assert.Equal(0L, result.RecordsRead)
    Assert.Equal(0L, result.RecordsAccepted)
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

[<Fact>]
let ``Vessel validate throws OperationCanceledException for already-cancelled token`` () = task {
    use cts  = new CancellationTokenSource()
    cts.Cancel()
    let ctx   = ExecutionContext.create cts.Token 1000
    let vessel = Vessel.validate "stage" ctx Helpers.positiveValidator
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let mutable threw = false
    try
        let! _ = vessel.Transform(input) |> TaskSeq.toListAsync
        ()
    with :? OperationCanceledException ->
        threw <- true

    Assert.True(threw)
}

[<Fact>]
let ``Pipeline runWithContext with FixedDelay(1) performs at most 2 total attempts`` () = task {
    let ctx = ExecutionContext.create CancellationToken.None 1000
    let mutable totalAttempts = 0
    let root : Root<int> = {
        Read = fun () -> taskSeq {
            totalAttempts <- totalAttempts + 1
            failwith "Simulated failure"
            yield 1
        }
    }
    let vessel = Vessel.map id
    let leaf, _ = Helpers.collectSink<int>()
    
    // FixedDelay(1, ...) means initial attempt + 1 retry = 2 total
    let ctxWithRetry = { ctx with RetryPolicy = FixedDelay(1, TimeSpan.FromMilliseconds(10.0)) }
    let! result = Pipeline.runWithContext ctxWithRetry root vessel leaf
    
    Assert.Equal(2, totalAttempts)
    Assert.Equal(1L, result.RecordsFailed)
}

[<Fact>]
let ``Pipeline runWithContext successful retry does not include pulses from failed attempt`` () = task {
    let ctx = ExecutionContext.create CancellationToken.None 1000
    let mutable totalAttempts = 0
    let root : Root<int> = {
        Read = fun () -> taskSeq {
            totalAttempts <- totalAttempts + 1
            if totalAttempts = 1 then
                failwith "First attempt fails"
            yield 1
        }
    }
    let vessel = Vessel.map id
    let leaf, _ = Helpers.collectSink<int>()
    
    let ctxWithRetry = { ctx with RetryPolicy = FixedDelay(1, TimeSpan.FromMilliseconds(10.0)) }
    let! result = Pipeline.runWithContext ctxWithRetry root vessel leaf
    
    Assert.Equal(2, totalAttempts)
    Assert.Equal(1L, result.RecordsRead)
    Assert.Equal(1L, result.RecordsAccepted)
    Assert.Equal(0L, result.RecordsFailed)
}
