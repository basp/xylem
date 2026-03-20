module FlowTests

open System
open System.Threading
open FSharp.Control
open Xunit
open Xylem
open Xylem.Domain
open Xylem.Flow

// ---------------------------------------------------------------------------
// Flow — map / filter / compose
// ---------------------------------------------------------------------------

[<Fact>]
let ``Flow map transforms every item`` () = task {
    let flow  = map (fun x -> x * 2)
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([2; 4; 6], result)
}

[<Fact>]
let ``Flow filter keeps only matching items`` () = task {
    let flow  = filter (fun x -> x % 2 = 0)
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([2; 4], result)
}

[<Fact>]
let ``Flow map on empty stream yields empty stream`` () = task {
    let flow = map (fun x -> x * 2)

    let! result = flow.Transform(TaskSeq.empty) |> TaskSeq.toListAsync

    Assert.Empty(result)
}

[<Fact>]
let ``Pipeline runWith threads source through flow into sink`` () = task {
    let source : Source<int> = {
        Read = fun () -> taskSeq { yield 1; yield 2; yield 3; yield 4; yield 5 }
    }
    let flow       = filter (fun x -> x % 2 <> 0)
    let sink, read = Helpers.collectSink<int>()

    do! Pipeline.runWith source flow sink

    Assert.Equal<int list>([1; 3; 5], read ())
}

[<Fact>]
let ``Flow map then filter via compose`` () = task {
    let flow  = map (fun x -> x * 2) >>> filter (fun x -> x > 4)
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([6], result)
}

// ---------------------------------------------------------------------------
// Flow.validate
// ---------------------------------------------------------------------------

[<Fact>]
let ``Flow validate passes all valid items through unchanged`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = validate "stage" ctx Helpers.positiveValidator
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([1; 2; 3], result)
}

[<Fact>]
let ``Flow validate filters out invalid items`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = validate "stage" ctx Helpers.positiveValidator
    let input = taskSeq { yield -1; yield 2; yield -3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([2; 4], result)
}

[<Fact>]
let ``Flow validate emits one Error event per rejected item`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = validate "validate-age" ctx Helpers.positiveValidator
    let input = taskSeq { yield -1; yield 2; yield -3 }

    let! _ = flow.Transform(input) |> TaskSeq.toListAsync
    let events = ctx.ReadEvents()

    Assert.Equal(2, events.Length)
    Assert.All(events, fun e -> Assert.Equal(Severity.Error, e.Severity))
    Assert.All(events, fun e -> Assert.Equal(Some "validate-age", e.Stage))
}

[<Fact>]
let ``Flow validate emits correct RecordIndex for each rejected item`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = validate "stage" ctx Helpers.positiveValidator
    // indices: 0 1 2
    let input = taskSeq { yield -1; yield 2; yield -3 }

    let! _ = flow.Transform(input) |> TaskSeq.toListAsync
    let events = ctx.ReadEvents()

    Assert.Equal(2, events.Length)
    Assert.Equal(Some 0L, events[0].RecordIndex)
    Assert.Equal(Some 2L, events[1].RecordIndex)
}

[<Fact>]
let ``Flow validate on empty stream emits no events`` () = task {
    let ctx  = ExecutionContext.``default`` ()
    let flow = validate "stage" ctx Helpers.positiveValidator

    let! _ = flow.Transform(TaskSeq.empty) |> TaskSeq.toListAsync

    Assert.Empty(ctx.ReadEvents())
}

[<Fact>]
let ``Flow validate with all-invalid stream yields empty output`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = validate "stage" ctx Helpers.positiveValidator
    let input = taskSeq { yield -1; yield -2; yield -3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Empty(result)
    Assert.Equal(3, ctx.ReadEvents().Length)
}

[<Fact>]
let ``Flow validate throws OperationCanceledException for already-cancelled token`` () = task {
    use cts  = new CancellationTokenSource()
    cts.Cancel()
    let ctx   = ExecutionContext.create cts.Token 1000
    let flow  = validate "stage" ctx Helpers.positiveValidator
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let mutable threw = false
    try
        let! _ = flow.Transform(input) |> TaskSeq.toListAsync
        ()
    with :? OperationCanceledException ->
        threw <- true

    Assert.True(threw)
}

// ---------------------------------------------------------------------------
// Flow.enrich
// ---------------------------------------------------------------------------

let private addLabelEnricher (x: int) : Result<string, ErrorKind> =
    if x > 0 then Ok $"item-{x}"
    else Result.Error (ValidationError("value", "must be positive to label"))

[<Fact>]
let ``Flow enrich passes all successfully enriched items through`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = enrich "stage" ctx addLabelEnricher
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<string list>(["item-1"; "item-2"; "item-3"], result)
}

[<Fact>]
let ``Flow enrich can change the item type`` () = task {
    let ctx      = ExecutionContext.``default`` ()
    let toLength = fun (s: string) -> Ok s.Length
    let flow     = enrich "stage" ctx toLength
    let input    = taskSeq { yield "ab"; yield "cde"; yield "f" }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([2; 3; 1], result)
}

[<Fact>]
let ``Flow enrich drops items where enrichment fails`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = enrich "stage" ctx addLabelEnricher
    let input = taskSeq { yield -1; yield 2; yield -3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<string list>(["item-2"; "item-4"], result)
}

[<Fact>]
let ``Flow enrich emits one Error event per failed enrichment`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = enrich "enrich-label" ctx addLabelEnricher
    let input = taskSeq { yield -1; yield 2; yield -3 }

    let! _ = flow.Transform(input) |> TaskSeq.toListAsync
    let events = ctx.ReadEvents()

    Assert.Equal(2, events.Length)
    Assert.All(events, fun e -> Assert.Equal(Severity.Error, e.Severity))
    Assert.All(events, fun e -> Assert.Equal(Some "enrich-label", e.Stage))
}

[<Fact>]
let ``Flow enrich emits correct RecordIndex for each failed item`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = enrich "stage" ctx addLabelEnricher
    // indices: 0 1 2
    let input = taskSeq { yield -1; yield 2; yield -3 }

    let! _ = flow.Transform(input) |> TaskSeq.toListAsync
    let events = ctx.ReadEvents()

    Assert.Equal(2, events.Length)
    Assert.Equal(Some 0L, events[0].RecordIndex)
    Assert.Equal(Some 2L, events[1].RecordIndex)
}

[<Fact>]
let ``Flow enrich on empty stream emits no events`` () = task {
    let ctx  = ExecutionContext.``default`` ()
    let flow = enrich "stage" ctx addLabelEnricher

    let! _ = flow.Transform(TaskSeq.empty) |> TaskSeq.toListAsync

    Assert.Empty(ctx.ReadEvents())
}

[<Fact>]
let ``Flow enrich with all-failing stream yields empty output`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = enrich "stage" ctx addLabelEnricher
    let input = taskSeq { yield -1; yield -2; yield -3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Empty(result)
    Assert.Equal(3, ctx.ReadEvents().Length)
}

[<Fact>]
let ``Flow enrich throws OperationCanceledException for already-cancelled token`` () = task {
    use cts  = new CancellationTokenSource()
    cts.Cancel()
    let ctx   = ExecutionContext.create cts.Token 1000
    let flow  = enrich "stage" ctx addLabelEnricher
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let mutable threw = false
    try
        let! _ = flow.Transform(input) |> TaskSeq.toListAsync
        ()
    with :? OperationCanceledException ->
        threw <- true

    Assert.True(threw)
}

// ---------------------------------------------------------------------------
// Flow.batch
// ---------------------------------------------------------------------------

[<Fact>]
let ``Flow batch groups items into full batches`` () = task {
    let flow  = batch 3
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4; yield 5; yield 6 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.Equal<int[]>([| 1; 2; 3 |], result[0])
    Assert.Equal<int[]>([| 4; 5; 6 |], result[1])
}

[<Fact>]
let ``Flow batch emits a partial final batch when items do not divide evenly`` () = task {
    let flow  = batch 3
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4; yield 5 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.Equal<int[]>([| 1; 2; 3 |], result[0])
    Assert.Equal<int[]>([| 4; 5 |],    result[1])
}

[<Fact>]
let ``Flow batch yields a single batch when items fewer than batchSize`` () = task {
    let flow  = batch 10
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(1, result.Length)
    Assert.Equal<int[]>([| 1; 2; 3 |], result[0])
}

[<Fact>]
let ``Flow batch on empty stream yields empty stream`` () = task {
    let flow = batch 3

    let! result = flow.Transform(TaskSeq.empty) |> TaskSeq.toListAsync

    Assert.Empty(result)
}

[<Fact>]
let ``Flow batch with batchSize 1 yields one item per batch`` () = task {
    let flow  = batch 1
    let input = taskSeq { yield "a"; yield "b"; yield "c" }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(3, result.Length)
    Assert.Equal<string[]>([| "a" |], result[0])
    Assert.Equal<string[]>([| "b" |], result[1])
    Assert.Equal<string[]>([| "c" |], result[2])
}

[<Fact>]
let ``Flow batch with exact multiple yields all full batches`` () = task {
    let flow  = batch 2
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.All(result, fun b -> Assert.Equal(2, b.Length))
}

[<Fact>]
let ``Flow batch throws ArgumentException for batchSize zero`` () =
    Assert.Throws<ArgumentException>(fun () -> batch 0 |> ignore)
    |> ignore

[<Fact>]
let ``Flow batch throws ArgumentException for negative batchSize`` () =
    Assert.Throws<ArgumentException>(fun () -> batch -1 |> ignore)
    |> ignore

[<Fact>]
let ``Flow batch can be composed with map`` () = task {
    let flow  = map (fun x -> x * 10) >>> batch 2
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.Equal<int[]>([| 10; 20 |], result[0])
    Assert.Equal<int[]>([| 30; 40 |], result[1])
}

[<Fact>]
let ``Flow batch respects ctx.BatchSize`` () = task {
    let ctx   = ExecutionContext.create CancellationToken.None 2
    let flow  = batch ctx.BatchSize
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.Equal<int[]>([| 1; 2 |], result[0])
    Assert.Equal<int[]>([| 3 |],    result[1])
}
