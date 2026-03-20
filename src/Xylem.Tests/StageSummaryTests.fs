module StageSummaryTests

open System
open Xunit
open Xylem
open Xylem.Domain

[<Fact>]
let ``summarizeByStage returns empty list for no events`` () =
    let result = PipelineResult.summarizeByStage []
    Assert.Empty(result)

[<Fact>]
let ``summarizeByStage groups events by stage`` () =
    let events = [
        { Helpers.makeEvent Error (ValidationError("Age", "negative")) with Stage = Some "validate" }
        { Helpers.makeEvent Error (ValidationError("Name", "empty"))   with Stage = Some "validate" }
        { Helpers.makeEvent Warning (Custom("coerced", Map.empty))     with Stage = Some "enrich" }
    ]
    let summaries = PipelineResult.summarizeByStage events
    Assert.Equal(2, summaries.Length)

    let validate = summaries |> List.find (fun s -> s.Stage = Some "validate")
    Assert.Equal(0L, validate.InfoCount)
    Assert.Equal(0L, validate.WarningCount)
    Assert.Equal(2L, validate.ErrorCount)
    Assert.Equal(0L, validate.FatalCount)
    Assert.Equal(2L, validate.TotalCount)

    let enrich = summaries |> List.find (fun s -> s.Stage = Some "enrich")
    Assert.Equal(0L, enrich.InfoCount)
    Assert.Equal(1L, enrich.WarningCount)
    Assert.Equal(0L, enrich.ErrorCount)
    Assert.Equal(0L, enrich.FatalCount)
    Assert.Equal(1L, enrich.TotalCount)

[<Fact>]
let ``summarizeByStage includes stageless events under None`` () =
    let events = [
        Helpers.makeEvent Fatal (SystemError(Exception("boom")))
        Helpers.makeEvent Warning (Custom("retry", Map.empty))
        { Helpers.makeEvent Error (ValidationError("x", "bad")) with Stage = Some "check" }
    ]
    let summaries = PipelineResult.summarizeByStage events
    Assert.Equal(2, summaries.Length)

    let pipeline = summaries |> List.find (fun s -> s.Stage = None)
    Assert.Equal(0L, pipeline.InfoCount)
    Assert.Equal(1L, pipeline.WarningCount)
    Assert.Equal(0L, pipeline.ErrorCount)
    Assert.Equal(1L, pipeline.FatalCount)
    Assert.Equal(2L, pipeline.TotalCount)

[<Fact>]
let ``summarizeByStage counts all severity levels`` () =
    let events = [
        { Helpers.makeEvent Info    (Custom("a", Map.empty)) with Stage = Some "s1" }
        { Helpers.makeEvent Warning (Custom("b", Map.empty)) with Stage = Some "s1" }
        { Helpers.makeEvent Error   (Custom("c", Map.empty)) with Stage = Some "s1" }
        { Helpers.makeEvent Fatal   (Custom("d", Map.empty)) with Stage = Some "s1" }
    ]
    let summaries = PipelineResult.summarizeByStage events
    Assert.Equal(1, summaries.Length)
    let s = summaries.Head
    Assert.Equal(1L, s.InfoCount)
    Assert.Equal(1L, s.WarningCount)
    Assert.Equal(1L, s.ErrorCount)
    Assert.Equal(1L, s.FatalCount)
    Assert.Equal(4L, s.TotalCount)

[<Fact>]
let ``summarizeByStage preserves stage ordering by first occurrence`` () =
    let events = [
        { Helpers.makeEvent Info (Custom("a", Map.empty)) with Stage = Some "second" }
        { Helpers.makeEvent Info (Custom("b", Map.empty)) with Stage = Some "first" }
        { Helpers.makeEvent Info (Custom("c", Map.empty)) with Stage = Some "second" }
    ]
    let summaries = PipelineResult.summarizeByStage events
    Assert.Equal(2, summaries.Length)
    Assert.Equal(Some "second", summaries[0].Stage)
    Assert.Equal(Some "first",  summaries[1].Stage)
