module RetryTests

open System
open System.Threading
open System.Threading.Tasks
open FSharp.Control
open Xunit
open Xylem
open Xylem.Biome

// ---------------------------------------------------------------------------
// NoRetry — preserves existing behaviour
// ---------------------------------------------------------------------------

[<Fact>]
let ``NoRetry succeeds normally`` () = task {
    let ctx    = ExecutionContext.``default`` ()
    let root : Root<int> = { Read = fun () -> taskSeq { yield 1; yield 2; yield 3 } }
    let vessel   = Vessel.map id
    let leaf, read = Helpers.collectSink<int> ()

    let! result = Pipeline.runWithContext ctx root vessel leaf

    Assert.Equal(3L, result.RecordsRead)
    Assert.Equal(3L, result.RecordsAccepted)
    Assert.Equal<int list>([1; 2; 3], read ())
}

[<Fact>]
let ``NoRetry emits Fatal on failure`` () = task {
    let ctx    = ExecutionContext.``default`` ()
    let root : Root<int> = { Read = fun () -> taskSeq { yield 1 } }
    let vessel   = Vessel.map id
    let leaf   : Leaf<int> = { Write = fun _ -> failwith "boom" }

    let! result = Pipeline.runWithContext ctx root vessel leaf

    Assert.Equal(1L, result.RecordsFailed)
    let fatal = result.Events |> List.find (fun e -> e.Severity = Fatal)
    match fatal.Kind with
    | RetryError (attempt, ex) ->
        Assert.Equal(1, attempt)
        Assert.Contains("boom", ex.Message)
    | _ -> Assert.Fail("expected RetryError")
}

// ---------------------------------------------------------------------------
// FixedDelay — retry scenarios
// ---------------------------------------------------------------------------

[<Fact>]
let ``FixedDelay succeeds on second attempt`` () = task {
    let mutable calls = 0
    let ctx = { ExecutionContext.``default`` () with RetryPolicy = FixedDelay(2, TimeSpan.Zero) }
    let root : Root<int> = { Read = fun () -> taskSeq { yield 42 } }
    let vessel   = Vessel.map id
    let leaf   : Leaf<int> = {
        Write = fun items -> task {
            calls <- calls + 1
            if calls = 1 then failwith "transient"
            do! items |> TaskSeq.iter ignore
        }
    }

    let! result = Pipeline.runWithContext ctx root vessel leaf

    Assert.Equal(2, calls)
    Assert.Equal(0L, result.RecordsFailed)
    let warnings = result.Events |> List.filter (fun e -> e.Severity = Warning)
    Assert.Single(warnings) |> ignore
    match warnings.Head.Kind with
    | RetryError (1, _) -> ()
    | _ -> Assert.Fail("expected RetryError attempt 1")
}

[<Fact>]
let ``FixedDelay exhausts all retries and emits Fatal`` () = task {
    let mutable calls = 0
    let ctx = { ExecutionContext.``default`` () with RetryPolicy = FixedDelay(2, TimeSpan.Zero) }
    let root : Root<int> = { Read = fun () -> taskSeq { yield 1 } }
    let vessel   = Vessel.map id
    let leaf   : Leaf<int> = {
        Write = fun _ -> task {
            calls <- calls + 1
            failwith $"fail-{calls}"
        }
    }

    let! result = Pipeline.runWithContext ctx root vessel leaf

    Assert.Equal(3, calls) // 1 initial + 2 retries
    Assert.Equal(1L, result.RecordsFailed)
    let warnings = result.Events |> List.filter (fun e -> e.Severity = Warning)
    Assert.Equal(2, warnings.Length)
    let fatal = result.Events |> List.filter (fun e -> e.Severity = Fatal)
    Assert.Single(fatal) |> ignore
}

[<Fact>]
let ``FixedDelay respects cancellation between retries`` () = task {
    use cts = new CancellationTokenSource()
    let ctx = { ExecutionContext.create cts.Token 1000 with RetryPolicy = FixedDelay(5, TimeSpan.FromSeconds 10.0) }
    let mutable calls = 0
    let root : Root<int> = { Read = fun () -> taskSeq { yield 1 } }
    let vessel   = Vessel.map id
    let leaf   : Leaf<int> = {
        Write = fun _ -> task {
            calls <- calls + 1
            // Cancel after the first failure, so the delay is canceled.
            cts.Cancel()
            failwith "fail"
        }
    }

    let mutable threw = false
    try
        let! _ = Pipeline.runWithContext ctx root vessel leaf
        ()
    with
    | :? OperationCanceledException
    | :? TaskCanceledException      -> threw <- true

    Assert.True(threw)
    Assert.Equal(1, calls)
}

[<Fact>]
let ``FixedDelay with zero maxAttempts behaves like NoRetry`` () = task {
    let mutable calls = 0
    let ctx = { ExecutionContext.``default`` () with RetryPolicy = FixedDelay(0, TimeSpan.Zero) }
    let root : Root<int> = { Read = fun () -> taskSeq { yield 1 } }
    let vessel   = Vessel.map id
    let leaf   : Leaf<int> = {
        Write = fun _ -> task {
            calls <- calls + 1
            failwith "boom"
        }
    }

    let! result = Pipeline.runWithContext ctx root vessel leaf

    Assert.Equal(1, calls)
    Assert.Equal(1L, result.RecordsFailed)
}
