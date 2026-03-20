module Tests

open System
open Xunit
open FSharp.Control
open Xylem
open Xylem.Connectors
open Xylem.Domain

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Alias for the in-memory sink — keeps test call-sites short.
let collectSink<'T> () = Connectors.InMemory.sink<'T> ()

let makeEvent severity kind =
    { Severity    = severity
      Kind        = kind
      Stage       = None
      RecordIndex = None
      Timestamp   = DateTimeOffset.UtcNow
      Message     = "test event" }

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
    Assert.NotSame(first, second)
}

[<Fact>]
let ``Source Read can yield zero items`` () = task {
    let source : Source<int> = {
        Read = fun () -> TaskSeq.empty
    }

    let! items = source.Read() |> TaskSeq.toListAsync

    Assert.Empty(items)
}

// ---------------------------------------------------------------------------
// Sink<'T>
// ---------------------------------------------------------------------------

[<Fact>]
let ``Sink Write receives all items from stream`` () = task {
    let stream = taskSeq { yield "a"; yield "b"; yield "c" }
    let sink, read = collectSink<string>()

    do! sink.Write(stream)

    Assert.Equal<string list>(["a"; "b"; "c"], read ())
}

[<Fact>]
let ``Sink Write receives empty stream without error`` () = task {
    let sink, read = collectSink<int>()

    do! sink.Write(TaskSeq.empty)

    Assert.Empty(read ())
}

// ---------------------------------------------------------------------------
// Pipeline.run — source -> sink
// ---------------------------------------------------------------------------

[<Fact>]
let ``Pipeline run passes source items to sink`` () = task {
    let source : Source<int> = {
        Read = fun () -> taskSeq { yield 10; yield 20; yield 30 }
    }
    let sink, read = collectSink<int>()

    do! Pipeline.run source sink

    Assert.Equal<int list>([10; 20; 30], read ())
}

[<Fact>]
let ``Pipeline run with empty source results in empty sink`` () = task {
    let source : Source<int> = { Read = fun () -> TaskSeq.empty }
    let sink, read = collectSink<int>()

    do! Pipeline.run source sink

    Assert.Empty(read ())
}

// ---------------------------------------------------------------------------
// Flow<'TIn,'TOut>
// ---------------------------------------------------------------------------

[<Fact>]
let ``Flow map transforms every item`` () = task {
    let flow = Flow.map (fun x -> x * 2)
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([2; 4; 6], result)
}

[<Fact>]
let ``Flow filter keeps only matching items`` () = task {
    let flow = Flow.filter (fun x -> x % 2 = 0)
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([2; 4], result)
}

[<Fact>]
let ``Flow map then filter via compose`` () =
    task {
        let flow =
            Flow.filter (fun x -> x > 4)
            |> Flow.compose (Flow.map (fun x -> x * 2))
        let input = taskSeq { yield 1; yield 2; yield 3 }

        let! result = flow.Transform(input) |> TaskSeq.toListAsync

        Assert.Equal<int list>([6], result)
    }

[<Fact>]
let ``Flow map on empty stream yields empty stream`` () = task {
    let flow = Flow.map (fun x -> x * 2)

    let! result = flow.Transform(TaskSeq.empty) |> TaskSeq.toListAsync

    Assert.Empty(result)
}

[<Fact>]
let ``Pipeline runWith threads source through flow into sink`` () = task {
    let source : Source<int> = {
        Read = fun () -> taskSeq { yield 1; yield 2; yield 3; yield 4; yield 5 }
    }
    let flow = Flow.filter (fun x -> x % 2 <> 0)
    let sink, read = collectSink<int>()

    do! Pipeline.runWith source flow sink

    Assert.Equal<int list>([1; 3; 5], read ())
}

// ---------------------------------------------------------------------------
// ExecutionContext
// ---------------------------------------------------------------------------

[<Fact>]
let ``ExecutionContext default starts with no events`` () =
    let ctx = ExecutionContext.``default`` ()
    Assert.Empty(ctx.ReadEvents())

[<Fact>]
let ``ExecutionContext Emit adds an event that ReadEvents returns`` () =
    let ctx = ExecutionContext.``default`` ()
    let event = makeEvent Warning (Custom("ping", Map.empty))
    ctx.Emit(event)
    let events = ctx.ReadEvents()
    Assert.Single(events) |> ignore
    Assert.Equal(Warning, events[0].Severity)

[<Fact>]
let ``ExecutionContext Emit accumulates multiple events in order`` () =
    let ctx = ExecutionContext.``default`` ()
    ctx.Emit(makeEvent Info    (Custom("a", Map.empty)))
    ctx.Emit(makeEvent Warning (Custom("b", Map.empty)))
    ctx.Emit(makeEvent Error   (Custom("c", Map.empty)))
    let events = ctx.ReadEvents()
    Assert.Equal(3, events.Length)
    Assert.Equal<Severity list>([Info; Warning; Error], events |> List.map _.Severity)

// ---------------------------------------------------------------------------
// Flow.validate
// ---------------------------------------------------------------------------

let positiveValidator (x: int) : Result<int, ErrorKind> =
    if x > 0 then Ok x
    else Result.Error (ValidationError("value", "must be positive"))

[<Fact>]
let ``Flow validate passes all valid items through unchanged`` () = task {
    let ctx = ExecutionContext.``default`` ()
    let flow = Flow.validate "stage" ctx positiveValidator
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([1; 2; 3], result)
}

[<Fact>]
let ``Flow validate filters out invalid items`` () = task {
    let ctx = ExecutionContext.``default`` ()
    let flow = Flow.validate "stage" ctx positiveValidator
    let input = taskSeq { yield -1; yield 2; yield -3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([2; 4], result)
}

[<Fact>]
let ``Flow validate emits one Error event per rejected item`` () = task {
    let ctx = ExecutionContext.``default`` ()
    let flow = Flow.validate "validate-age" ctx positiveValidator
    let input = taskSeq { yield -1; yield 2; yield -3 }

    let! _ = flow.Transform(input) |> TaskSeq.toListAsync
    let events = ctx.ReadEvents()

    Assert.Equal(2, events.Length)
    Assert.All(events, fun e -> Assert.Equal(Severity.Error, e.Severity))
    Assert.All(events, fun e -> Assert.Equal(Some "validate-age", e.Stage))
}

[<Fact>]
let ``Flow validate emits correct RecordIndex for each rejected item`` () = task {
    let ctx = ExecutionContext.``default`` ()
    let flow = Flow.validate "stage" ctx positiveValidator
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
    let ctx = ExecutionContext.``default`` ()
    let flow = Flow.validate "stage" ctx positiveValidator

    let! _ = flow.Transform(TaskSeq.empty) |> TaskSeq.toListAsync

    Assert.Empty(ctx.ReadEvents())
}

[<Fact>]
let ``Flow validate with all-invalid stream yields empty output`` () = task {
    let ctx = ExecutionContext.``default`` ()
    let flow = Flow.validate "stage" ctx positiveValidator
    let input = taskSeq { yield -1; yield -2; yield -3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Empty(result)
    Assert.Equal(3, ctx.ReadEvents().Length)
}

// ---------------------------------------------------------------------------
// Pipeline.runWithContext
// ---------------------------------------------------------------------------

[<Fact>]
let ``Pipeline runWithContext returns correct RecordsRead when all pass`` () = task {
    let ctx = ExecutionContext.``default`` ()
    let source : Source<int> = { Read = fun () -> taskSeq { yield 1; yield 2; yield 3 } }
    let flow = Flow.map id
    let sink, _ = collectSink<int>()

    let! result = Pipeline.runWithContext ctx source flow sink

    Assert.Equal(3L, result.RecordsRead)
    Assert.Equal(3L, result.RecordsAccepted)
    Assert.Equal(0L, result.RecordsRejected)
    Assert.Equal(0L, result.RecordsFailed)
}

[<Fact>]
let ``Pipeline runWithContext captures validation rejections in PipelineResult`` () = task {
    let ctx = ExecutionContext.``default`` ()
    let source : Source<int> = { Read = fun () -> taskSeq { yield -1; yield 2; yield -3; yield 4 } }
    let flow = Flow.validate "stage" ctx positiveValidator
    let sink, read = collectSink<int>()

    let! result = Pipeline.runWithContext ctx source flow sink

    Assert.Equal(4L, result.RecordsRead)
    Assert.Equal(2L, result.RecordsRejected)
    Assert.Equal(2L, result.RecordsAccepted)
    Assert.Equal(0L, result.RecordsFailed)
    Assert.Equal<int list>([2; 4], read ())
}

[<Fact>]
let ``Pipeline runWithContext records a non-negative Duration`` () = task {
    let ctx = ExecutionContext.``default`` ()
    let source : Source<int> = { Read = fun () -> TaskSeq.empty }
    let flow = Flow.map id
    let sink, _ = collectSink<int>()

    let! result = Pipeline.runWithContext ctx source flow sink

    Assert.True(result.Duration >= TimeSpan.Zero)
}

[<Fact>]
let ``Pipeline runWithContext with empty source returns zero counts`` () = task {
    let ctx = ExecutionContext.``default`` ()
    let source : Source<int> = { Read = fun () -> TaskSeq.empty }
    let flow = Flow.map id
    let sink, _ = collectSink<int>()

    let! result = Pipeline.runWithContext ctx source flow sink

    Assert.Equal(0L, result.RecordsRead)
    Assert.Equal(0L, result.RecordsAccepted)
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

[<Fact>]
let ``Flow validate throws OperationCanceledException for already-cancelled token`` () = task {
    use cts = new System.Threading.CancellationTokenSource()
    cts.Cancel()
    let ctx   = ExecutionContext.create cts.Token 1000
    let flow  = Flow.validate "stage" ctx positiveValidator
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
    use cts = new System.Threading.CancellationTokenSource()
    cts.Cancel()
    let ctx    = ExecutionContext.create cts.Token 1000
    let source : Source<int> = { Read = fun () -> taskSeq { yield 1; yield 2; yield 3 } }
    let flow   = Flow.map id
    let sink, _ = collectSink<int>()

    let mutable threw = false
    try
        let! _ = Pipeline.runWithContext ctx source flow sink
        ()
    with :? OperationCanceledException ->
        threw <- true

    Assert.True(threw)
}

// ---------------------------------------------------------------------------
// Flow.enrich
// ---------------------------------------------------------------------------

let addLabelEnricher (x: int) : Result<string, ErrorKind> =
    if x > 0 then Ok $"item-{x}"
    else Result.Error (ValidationError("value", "must be positive to label"))

[<Fact>]
let ``Flow enrich passes all successfully enriched items through`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = Flow.enrich "stage" ctx addLabelEnricher
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<string list>(["item-1"; "item-2"; "item-3"], result)
}

[<Fact>]
let ``Flow enrich can change the item type`` () = task {
    let ctx      = ExecutionContext.``default`` ()
    let toLength = fun (s: string) -> Ok s.Length
    let flow     = Flow.enrich "stage" ctx toLength
    let input    = taskSeq { yield "ab"; yield "cde"; yield "f" }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<int list>([2; 3; 1], result)
}

[<Fact>]
let ``Flow enrich drops items where enrichment fails`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = Flow.enrich "stage" ctx addLabelEnricher
    let input = taskSeq { yield -1; yield 2; yield -3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal<string list>(["item-2"; "item-4"], result)
}

[<Fact>]
let ``Flow enrich emits one Error event per failed enrichment`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = Flow.enrich "enrich-label" ctx addLabelEnricher
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
    let flow  = Flow.enrich "stage" ctx addLabelEnricher
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
    let flow = Flow.enrich "stage" ctx addLabelEnricher

    let! _ = flow.Transform(TaskSeq.empty) |> TaskSeq.toListAsync

    Assert.Empty(ctx.ReadEvents())
}

[<Fact>]
let ``Flow enrich with all-failing stream yields empty output`` () = task {
    let ctx   = ExecutionContext.``default`` ()
    let flow  = Flow.enrich "stage" ctx addLabelEnricher
    let input = taskSeq { yield -1; yield -2; yield -3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Empty(result)
    Assert.Equal(3, ctx.ReadEvents().Length)
}

[<Fact>]
let ``Flow enrich throws OperationCanceledException for already-cancelled token`` () = task {
    use cts  = new System.Threading.CancellationTokenSource()
    cts.Cancel()
    let ctx   = ExecutionContext.create cts.Token 1000
    let flow  = Flow.enrich "stage" ctx addLabelEnricher
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
    let flow  = Flow.batch 3
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4; yield 5; yield 6 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.Equal<int[]>([| 1; 2; 3 |], result[0])
    Assert.Equal<int[]>([| 4; 5; 6 |], result[1])
}

[<Fact>]
let ``Flow batch emits a partial final batch when items do not divide evenly`` () = task {
    let flow  = Flow.batch 3
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4; yield 5 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.Equal<int[]>([| 1; 2; 3 |], result[0])
    Assert.Equal<int[]>([| 4; 5 |],    result[1])
}

[<Fact>]
let ``Flow batch yields a single batch when items fewer than batchSize`` () = task {
    let flow  = Flow.batch 10
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(1, result.Length)
    Assert.Equal<int[]>([| 1; 2; 3 |], result[0])
}

[<Fact>]
let ``Flow batch on empty stream yields empty stream`` () = task {
    let flow = Flow.batch 3

    let! result = flow.Transform(TaskSeq.empty) |> TaskSeq.toListAsync

    Assert.Empty(result)
}

[<Fact>]
let ``Flow batch with batchSize 1 yields one item per batch`` () = task {
    let flow  = Flow.batch 1
    let input = taskSeq { yield "a"; yield "b"; yield "c" }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(3, result.Length)
    Assert.Equal<string[]>([| "a" |], result[0])
    Assert.Equal<string[]>([| "b" |], result[1])
    Assert.Equal<string[]>([| "c" |], result[2])
}

[<Fact>]
let ``Flow batch with exact multiple yields all full batches`` () = task {
    let flow  = Flow.batch 2
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.All(result, fun b -> Assert.Equal(2, b.Length))
}

[<Fact>]
let ``Flow batch throws ArgumentException for batchSize zero`` () =
    Assert.Throws<ArgumentException>(fun () -> Flow.batch 0 |> ignore)
    |> ignore

[<Fact>]
let ``Flow batch throws ArgumentException for negative batchSize`` () =
    Assert.Throws<ArgumentException>(fun () -> Flow.batch -1 |> ignore)
    |> ignore

[<Fact>]
let ``Flow batch can be composed with map`` () = task {
    // map first, then batch
    let flow  = Flow.compose (Flow.map (fun x -> x * 10)) (Flow.batch 2)
    let input = taskSeq { yield 1; yield 2; yield 3; yield 4 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.Equal<int[]>([| 10; 20 |], result[0])
    Assert.Equal<int[]>([| 30; 40 |], result[1])
}

[<Fact>]
let ``Flow batch respects ctx.BatchSize`` () = task {
    let ctx   = ExecutionContext.create System.Threading.CancellationToken.None 2
    let flow  = Flow.batch ctx.BatchSize
    let input = taskSeq { yield 1; yield 2; yield 3 }

    let! result = flow.Transform(input) |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.Equal<int[]>([| 1; 2 |], result[0])
    Assert.Equal<int[]>([| 3 |],    result[1])
}

// ---------------------------------------------------------------------------
// Connectors.InMemory
// ---------------------------------------------------------------------------

[<Fact>]
let ``InMemory source yields all items from a list`` () = task {
    let source = Connectors.InMemory.source [1; 2; 3]

    let! items = source.Read() |> TaskSeq.toListAsync

    Assert.Equal<int list>([1; 2; 3], items)
}

[<Fact>]
let ``InMemory source yields all items from an array`` () = task {
    let source = Connectors.InMemory.source [| "a"; "b"; "c" |]

    let! items = source.Read() |> TaskSeq.toListAsync

    Assert.Equal<string list>(["a"; "b"; "c"], items)
}

[<Fact>]
let ``InMemory source from empty sequence yields empty stream`` () = task {
    let source = Connectors.InMemory.source ([] : int list)

    let! items = source.Read() |> TaskSeq.toListAsync

    Assert.Empty(items)
}

[<Fact>]
let ``InMemory source Read called twice produces independent streams`` () = task {
    let source = Connectors.InMemory.source [1; 2; 3]

    let! first  = source.Read() |> TaskSeq.toListAsync
    let! second = source.Read() |> TaskSeq.toListAsync

    Assert.Equal<int list>(first, second)
    Assert.NotSame(first, second)
}

[<Fact>]
let ``InMemory sink collects all written items`` () = task {
    let sink, read = Connectors.InMemory.sink ()
    let stream = taskSeq { yield 1; yield 2; yield 3 }

    do! sink.Write(stream)

    Assert.Equal<int list>([1; 2; 3], read ())
}

[<Fact>]
let ``InMemory sink snapshot is empty before write`` () =
    let _, read = Connectors.InMemory.sink<int> ()

    Assert.Empty(read ())

[<Fact>]
let ``InMemory sink write with empty stream yields empty snapshot`` () = task {
    let sink, read = Connectors.InMemory.sink<int> ()

    do! sink.Write(TaskSeq.empty)

    Assert.Empty(read ())
}

[<Fact>]
let ``InMemory sink read returns a fresh snapshot each call`` () = task {
    let sink, read = Connectors.InMemory.sink ()
    let stream = taskSeq { yield "x"; yield "y" }

    do! sink.Write(stream)
    let snap1 = read ()
    let snap2 = read ()

    Assert.Equal<string list>(snap1, snap2)
    Assert.NotSame(snap1, snap2)
}

[<Fact>]
let ``InMemory source and sink round-trip a full pipeline`` () = task {
    let source      = Connectors.InMemory.source [1; 2; 3; 4; 5]
    let flow        = Flow.filter (fun x -> x % 2 = 0)
    let sink, read  = Connectors.InMemory.sink ()

    do! Pipeline.runWith source flow sink

    Assert.Equal<int list>([2; 4], read ())
}

[<Fact>]
let ``DiagnosticEvent can be constructed for each Severity`` () =
    let severities = [ Info; Warning; Error; Fatal ]
    let events = severities |> List.map (fun s -> makeEvent s (Custom("test", Map.empty)))
    Assert.Equal(4, events.Length)
    Assert.Equal<Severity list>(severities, events |> List.map _.Severity)

[<Fact>]
let ``ErrorKind cases are all pattern-matchable`` () =
    let exn = Exception("boom")
    let kinds = [
        SystemError exn
        IoError("/some/path", exn)
        ValidationError("Age", "must be >= 0")
        BusinessRuleViolation("MAX_ORDER_LINES", "exceeded limit of 100")
        PipelineError("validate-stage", exn)
        Custom("my-domain-error", Map.ofList ["key", "value"])
    ]
    let labels =
        kinds |> List.map (fun k ->
            match k with
            | SystemError _               -> "system"
            | IoError _                   -> "io"
            | ValidationError _           -> "validation"
            | BusinessRuleViolation _     -> "business"
            | PipelineError _             -> "pipeline"
            | Custom _                    -> "custom")
    Assert.Equal<string list>(
        ["system"; "io"; "validation"; "business"; "pipeline"; "custom"],
        labels)

[<Fact>]
let ``Custom ErrorKind round-trips tag and data`` () =
    let data = Map.ofList ["orderId", "42"; "reason", "duplicate"]
    let kind = Custom("ORDER_ERROR", data)
    match kind with
    | Custom(tag, d) ->
        Assert.Equal("ORDER_ERROR", tag)
        Assert.Equal("42", d["orderId"])
        Assert.Equal("duplicate", d["reason"])
    | _ -> Assert.Fail("expected Custom")

[<Fact>]
let ``DiagnosticEvent Stage and RecordIndex are optional`` () =
    let withBoth =
        { makeEvent Warning (ValidationError("f", "r")) with
            Stage       = Some "my-flow"
            RecordIndex = Some 7L }
    Assert.Equal(Some "my-flow", withBoth.Stage)
    Assert.Equal(Some 7L, withBoth.RecordIndex)

    let withNeither = makeEvent Info (Custom("ping", Map.empty))
    Assert.Equal(None, withNeither.Stage)
    Assert.Equal(None, withNeither.RecordIndex)

[<Fact>]
let ``PipelineResult empty has zero counts and no events`` () =
    let r = PipelineResult.empty
    Assert.Equal(0L, r.RecordsRead)
    Assert.Equal(0L, r.RecordsAccepted)
    Assert.Equal(0L, r.RecordsRejected)
    Assert.Equal(0L, r.RecordsFailed)
    Assert.Empty(r.Events)

[<Fact>]
let ``PipelineResult fromEvents computes counts from event list`` () =
    let events = [
        makeEvent Error  (ValidationError("Age", "negative"))      // rejected
        makeEvent Error  (ValidationError("Name", "empty"))        // rejected
        makeEvent Fatal  (SystemError(Exception("disk")))   // failed
        makeEvent Warning (Custom("coerced", Map.empty))           // warning — not a rejection
    ]
    let result = PipelineResult.fromEvents 10L (TimeSpan.FromSeconds 1.0) events
    Assert.Equal(10L, result.RecordsRead)
    Assert.Equal(2L,  result.RecordsRejected)   // Error severity
    Assert.Equal(1L,  result.RecordsFailed)     // Fatal severity
    Assert.Equal(4,   result.Events.Length)
