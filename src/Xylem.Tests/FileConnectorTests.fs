module FileConnectorTests

open System
open System.IO
open FSharp.Control
open Xunit
open Xylem
open Xylem.Connectors
open Xylem.Domain

/// <summary>
/// Reads all lines written to a <c>StringWriter</c>, mirroring how
/// <c>TextReader.ReadLine</c> works in <c>File.sourceFrom</c>.
/// </summary>
let private readWrittenLines (sw: StringWriter) =
    use reader = new StringReader(sw.ToString())
    seq {
        let mutable line = reader.ReadLine()
        while not (isNull line) do
            yield line
            line <- reader.ReadLine()
    } |> List.ofSeq

// ---------------------------------------------------------------------------
// File.sourceFrom
// ---------------------------------------------------------------------------

[<Fact>]
let ``File sourceFrom yields all lines`` () = task {
    let source = File.sourceFrom (fun () -> new StringReader("alice\nbob\ncarol"))

    let! lines = source.Read() |> TaskSeq.toListAsync

    Assert.Equal<string list>(["alice"; "bob"; "carol"], lines)
}

[<Fact>]
let ``File sourceFrom preserves empty lines`` () = task {
    let source = File.sourceFrom (fun () -> new StringReader("first\n\nthird"))

    let! lines = source.Read() |> TaskSeq.toListAsync

    Assert.Equal<string list>(["first"; ""; "third"], lines)
}

[<Fact>]
let ``File sourceFrom empty content yields empty stream`` () = task {
    let source = File.sourceFrom (fun () -> new StringReader(""))

    let! lines = source.Read() |> TaskSeq.toListAsync

    Assert.Empty(lines)
}

[<Fact>]
let ``File sourceFrom Read called twice calls factory twice`` () = task {
    let mutable callCount = 0
    let source = File.sourceFrom (fun () ->
        callCount <- callCount + 1
        new StringReader("x\ny") :> TextReader)

    let! first  = source.Read() |> TaskSeq.toListAsync
    let! second = source.Read() |> TaskSeq.toListAsync

    Assert.Equal(2, callCount)
    Assert.Equal<string list>(first, second)
}

[<Fact>]
let ``File sourceFrom factory throwing surfaces as Fatal in PipelineResult`` () = task {
    let ctx    = ExecutionContext.``default`` ()
    let source : Source<string> = File.sourceFrom (fun () -> failwith "cannot open file")
    let flow   = Flow.map id
    let sink, _ = Helpers.collectSink<string>()

    let! result = Pipeline.runWithContext ctx source flow sink

    Assert.Equal(1L, result.RecordsFailed)
    Assert.Equal(Fatal, result.Events[0].Severity)
}

// ---------------------------------------------------------------------------
// File.sinkFrom
// ---------------------------------------------------------------------------

[<Fact>]
let ``File sinkFrom writes all strings as lines`` () = task {
    let sw   = new StringWriter()
    let sink = File.sinkFrom (fun () -> sw :> TextWriter)

    do! sink.Write(taskSeq { yield "line1"; yield "line2"; yield "line3" })

    Assert.Equal<string list>(["line1"; "line2"; "line3"], readWrittenLines sw)
}

[<Fact>]
let ``File sinkFrom empty stream writes nothing`` () = task {
    let sw   = new StringWriter()
    let sink = File.sinkFrom (fun () -> sw :> TextWriter)

    do! sink.Write(TaskSeq.empty)

    Assert.Empty(readWrittenLines sw)
}

[<Fact>]
let ``File sourceFrom and sinkFrom round-trip all lines`` () = task {
    let lines = ["alpha"; "beta"; "gamma"]
    let sw    = new StringWriter()
    let sink  = File.sinkFrom (fun () -> sw :> TextWriter)
    do! sink.Write(taskSeq { for l in lines do yield l })

    let source = File.sourceFrom (fun () -> new StringReader(sw.ToString()))
    let! result = source.Read() |> TaskSeq.toListAsync

    Assert.Equal<string list>(lines, result)
}

// ---------------------------------------------------------------------------
// File — integration
// ---------------------------------------------------------------------------

[<Fact>]
let ``File sourceFrom and sinkFrom round-trip through a flow`` () = task {
    let ctx    = ExecutionContext.``default`` ()
    let source = File.sourceFrom (fun () -> new StringReader("1\n2\nbad\n3"))
    let sw     = new StringWriter()
    let sink   = File.sinkFrom (fun () -> sw :> TextWriter)
    let flow =
        Flow.compose
            (Flow.enrich "parse-int" ctx (fun (line: string) ->
                match Int32.TryParse(line) with
                | true, n -> Ok n
                | _       -> Result.Error (ValidationError("line", $"'{line}' is not an integer"))))
            (Flow.map string)

    let! result = Pipeline.runWithContext ctx source flow sink

    Assert.Equal(4L, result.RecordsRead)
    Assert.Equal(3L, result.RecordsAccepted)
    Assert.Equal(1L, result.RecordsRejected)
    Assert.Equal<string list>(["1"; "2"; "3"], readWrittenLines sw)
}
