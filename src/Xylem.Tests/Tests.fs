module Tests

open System
open System.Collections.Generic
open Xunit
open FSharp.Control
open Xylem
open Xylem.Domain

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// In-memory sink that accumulates items into a List for assertion.
let collectSink<'T> () =
    let collected = List<'T>()
    let sink : Sink<'T> = {
        Write = fun stream -> task {
            do! stream |> TaskSeq.iter collected.Add
        }
    }
    sink, collected

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
    let sink, collected = collectSink<string>()

    do! sink.Write(stream)

    Assert.Equal<string list>(["a"; "b"; "c"], List.ofSeq collected)
}

[<Fact>]
let ``Sink Write receives empty stream without error`` () = task {
    let sink, collected = collectSink<int>()

    do! sink.Write(TaskSeq.empty)

    Assert.Empty(collected)
}

// ---------------------------------------------------------------------------
// Pipeline.run — source -> sink
// ---------------------------------------------------------------------------

[<Fact>]
let ``Pipeline run passes source items to sink`` () = task {
    let source : Source<int> = {
        Read = fun () -> taskSeq { yield 10; yield 20; yield 30 }
    }
    let sink, collected = collectSink<int>()

    do! Pipeline.run source sink

    Assert.Equal<int list>([10; 20; 30], List.ofSeq collected)
}

[<Fact>]
let ``Pipeline run with empty source results in empty sink`` () = task {
    let source : Source<int> = { Read = fun () -> TaskSeq.empty }
    let sink, collected = collectSink<int>()

    do! Pipeline.run source sink

    Assert.Empty(collected)
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
    let sink, collected = collectSink<int>()

    do! Pipeline.runWith source flow sink

    Assert.Equal<int list>([1; 3; 5], List.ofSeq collected)
}

// ---------------------------------------------------------------------------
// Diagnostics — Severity, ErrorKind, DiagnosticEvent, PipelineResult
// ---------------------------------------------------------------------------

let makeEvent severity kind =
    { Severity    = severity
      Kind        = kind
      Stage       = None
      RecordIndex = None
      Timestamp   = DateTimeOffset.UtcNow
      Message     = "test event" }

[<Fact>]
let ``DiagnosticEvent can be constructed for each Severity`` () =
    let severities = [ Info; Warning; Error; Fatal ]
    let events = severities |> List.map (fun s -> makeEvent s (Custom("test", Map.empty)))
    Assert.Equal(4, events.Length)
    Assert.Equal<Severity list>(severities, events |> List.map _.Severity)

[<Fact>]
let ``ErrorKind cases are all pattern-matchable`` () =
    let exn = System.Exception("boom")
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
        makeEvent Fatal  (SystemError(System.Exception("disk")))   // failed
        makeEvent Warning (Custom("coerced", Map.empty))           // warning — not a rejection
    ]
    let result = PipelineResult.fromEvents 10L (TimeSpan.FromSeconds 1.0) events
    Assert.Equal(10L, result.RecordsRead)
    Assert.Equal(2L,  result.RecordsRejected)   // Error severity
    Assert.Equal(1L,  result.RecordsFailed)     // Fatal severity
    Assert.Equal(4,   result.Events.Length)
