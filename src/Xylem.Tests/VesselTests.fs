module VesselTests

open System
open System.Threading
open FSharp.Control
open Xunit
open Xylem
open Xylem.Domain
open Xylem.Vessel

// ---------------------------------------------------------------------------
// Flow — map / filter / compose
// ---------------------------------------------------------------------------

[<Fact>]
let ``Vessel map transforms every item`` () = task {
    let flow  = map (fun x -> x * 2)
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([2; 4; 6], result)
}

[<Fact>]
let ``Vessel filter keeps only matching items`` () = task {
    let flow  = filter (fun x -> x % 2 = 0)
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([2; 4], result)
}

[<Fact>]
let ``Vessel map on empty stream yields empty stream`` () = task {
    let flow = map (fun x -> x * 2)

    let! result = flow.Transform(TaskSeq.empty) |> TaskSeq.toListAsync

    Assert.Empty(result)
}

[<Fact>]
let ``Pipeline runWith threads root through vessel into leaf`` () = task {
    let root : Root<int> = {
        Read = fun () -> taskSeq { yield 1; yield 2; yield 3; yield 4; yield 5 }
    }
    let vessel     = filter (fun x -> x % 2 <> 0)
    let leaf, read = Helpers.collectSink<int>()

    do! Pipeline.runWith root vessel leaf

    Assert.Equal<int list>([1; 3; 5], read ())
}

[<Fact>]
let ``Vessel map then filter via compose`` () = task {
    let flow  = map (fun x -> x * 2) >>> filter (fun x -> x > 4)
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([6], result)
}

// ---------------------------------------------------------------------------
// Vessel.validate
// ---------------------------------------------------------------------------

[<Fact>]
let ``Vessel validate passes all valid items through unchanged`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = validate "stage" ctx Helpers.positiveValidator
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([1; 2; 3], result)
}

[<Fact>]
let ``Vessel validate filters out invalid items`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = validate "stage" ctx Helpers.positiveValidator
    let input = taskSeq { yield -1; yield 2; yield -3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([2; 4], result)
}

[<Fact>]
let ``Vessel validate emits one Error event per rejected item`` () = task {
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
let ``Vessel validate emits correct RecordIndex for each rejected item`` () = task {
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
let ``Vessel validate on empty stream emits no events`` () = task {
    let ctx  = ExecutionContext.``default`` ()
    let flow = validate "stage" ctx Helpers.positiveValidator

    let! _ = flow.Transform(TaskSeq.empty) |> TaskSeq.toListAsync

    Assert.Empty(ctx.ReadEvents())
}

[<Fact>]
let ``Vessel validate with all-invalid stream yields empty output`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = validate "stage" ctx Helpers.positiveValidator
    let input = taskSeq { yield -1; yield -2; yield -3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Empty(result)
    Assert.Equal(3, ctx.ReadEvents().Length)
}

[<Fact>]
let ``Vessel validate throws OperationCanceledException for already-cancelled token`` () = task {
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
// Vessel.enrich
// ---------------------------------------------------------------------------

let private addLabelEnricher (x: int) : Result<string, ErrorKind> =
    if x > 0 then Ok $"item-{x}"
    else Result.Error (ValidationError("value", "must be positive to label"))

[<Fact>]
let ``Vessel enrich passes all successfully enriched items through`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = enrich "stage" ctx addLabelEnricher
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<string list>(["item-1"; "item-2"; "item-3"], result)
}

[<Fact>]
let ``Vessel enrich can change the item type`` () = task {
    let ctx      = ExecutionContext.``default`` ()
    let toLength = fun (s: string) -> Ok s.Length
    let flow     = enrich "stage" ctx toLength
    let input    = taskSeq { yield "ab"; yield "cde"; yield "f" }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([2; 3; 1], result)
}

[<Fact>]
let ``Vessel enrich drops items where enrichment fails`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = enrich "stage" ctx addLabelEnricher
    let input = taskSeq { yield -1; yield 2; yield -3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<string list>(["item-2"; "item-4"], result)
}

[<Fact>]
let ``Vessel enrich emits one Error event per failed enrichment`` () = task {
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
let ``Vessel enrich emits correct RecordIndex for each failed item`` () = task {
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
let ``Vessel enrich on empty stream emits no events`` () = task {
    let ctx  = ExecutionContext.``default`` ()
    let flow = enrich "stage" ctx addLabelEnricher

    let! _ = flow.Transform(TaskSeq.empty) |> TaskSeq.toListAsync

    Assert.Empty(ctx.ReadEvents())
}

[<Fact>]
let ``Vessel enrich with all-failing stream yields empty output`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = enrich "stage" ctx addLabelEnricher
    let input = taskSeq { yield -1; yield -2; yield -3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Empty(result)
    Assert.Equal(3, ctx.ReadEvents().Length)
}

[<Fact>]
let ``Vessel enrich throws OperationCanceledException for already-cancelled token`` () = task {
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
// Vessel.batch
// ---------------------------------------------------------------------------

[<Fact>]
let ``Vessel batch groups items into full batches`` () = task {
    let flow  = batch 3
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4; yield 5; yield 6 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.Equal<int[]>([| 1; 2; 3 |], result[0])
    Assert.Equal<int[]>([| 4; 5; 6 |], result[1])
}

[<Fact>]
let ``Vessel batch emits a partial final batch when items do not divide evenly`` () = task {
    let flow  = batch 3
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4; yield 5 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.Equal<int[]>([| 1; 2; 3 |], result[0])
    Assert.Equal<int[]>([| 4; 5 |],    result[1])
}

[<Fact>]
let ``Vessel batch yields a single batch when items fewer than batchSize`` () = task {
    let flow  = batch 10
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(1, result.Length)
    Assert.Equal<int[]>([| 1; 2; 3 |], result[0])
}

[<Fact>]
let ``Vessel batch on empty stream yields empty stream`` () = task {
    let flow = batch 3

    let! result = flow.Transform(TaskSeq.empty) |> TaskSeq.toListAsync

    Assert.Empty(result)
}

[<Fact>]
let ``Vessel batch with batchSize 1 yields one item per batch`` () = task {
    let flow  = batch 1
    let input = taskSeq { yield "a"; yield "b"; yield "c" }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(3, result.Length)
    Assert.Equal<string[]>([| "a" |], result[0])
    Assert.Equal<string[]>([| "b" |], result[1])
    Assert.Equal<string[]>([| "c" |], result[2])
}

[<Fact>]
let ``Vessel batch with exact multiple yields all full batches`` () = task {
    let flow  = batch 2
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.All(result, fun b -> Assert.Equal(2, b.Length))
}

[<Fact>]
let ``Vessel batch throws ArgumentException for batchSize zero`` () =
    Assert.Throws<ArgumentException>(fun () -> batch 0 |> ignore)
    |> ignore

[<Fact>]
let ``Vessel batch throws ArgumentException for negative batchSize`` () =
    Assert.Throws<ArgumentException>(fun () -> batch -1 |> ignore)
    |> ignore

[<Fact>]
let ``Vessel batch can be composed with map`` () = task {
    let flow  = map (fun x -> x * 10) >>> batch 2
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.Equal<int[]>([| 10; 20 |], result[0])
    Assert.Equal<int[]>([| 30; 40 |], result[1])
}

[<Fact>]
let ``Vessel batch respects ctx.BatchSize`` () = task {
    let ctx   = ExecutionContext.create CancellationToken.None 2
    let flow  = batch ctx.BatchSize
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.Equal<int[]>([| 1; 2 |], result[0])
    Assert.Equal<int[]>([| 3 |],    result[1])
}
