module PulseTests

open System
open Xunit
open Xylem
open Xylem.Biome

[<Fact>]
let ``Pulse can be constructed for each Severity`` () =
    let severities = [ Info; Warning; Error; Fatal ]
    let events     = severities |> List.map (fun s -> Helpers.makeEvent s (Custom("test", Map.empty)))
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
        Custom("my-biome-error", Map.ofList ["key", "value"])
        RetryError(1, exn)
    ]
    let labels =
        kinds |> List.map (fun k ->
            match k with
            | SystemError _           -> "system"
            | IoError _               -> "io"
            | ValidationError _       -> "validation"
            | BusinessRuleViolation _ -> "business"
            | PipelineError _         -> "pipeline"
            | Custom _                -> "custom"
            | RetryError _            -> "retry")
    Assert.Equal<string list>(
        ["system"; "io"; "validation"; "business"; "pipeline"; "custom"; "retry"],
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
let ``Pulse Stage and RecordIndex are optional`` () =
    let withBoth =
        { Helpers.makeEvent Warning (ValidationError("f", "r")) with
            Stage       = Some "my-flow"
            RecordIndex = Some 7L }
    Assert.Equal(Some "my-flow", withBoth.Stage)
    Assert.Equal(Some 7L, withBoth.RecordIndex)

    let withNeither = Helpers.makeEvent Info (Custom("ping", Map.empty))
    Assert.Equal(None, withNeither.Stage)
    Assert.Equal(None, withNeither.RecordIndex)

[<Fact>]
let ``Harvest empty has zero counts and no events`` () =
    let r = Harvest.empty
    Assert.Equal(0L, r.RecordsRead)
    Assert.Equal(0L, r.RecordsAccepted)
    Assert.Equal(0L, r.RecordsRejected)
    Assert.Equal(0L, r.RecordsFailed)
    Assert.Empty(r.Events)

[<Fact>]
let ``Harvest fromEvents computes counts from event list`` () =
    let events = [
        Helpers.makeEvent Error  (ValidationError("Age", "negative"))           // rejected
        Helpers.makeEvent Error  (ValidationError("Name", "empty"))             // rejected
        Helpers.makeEvent Fatal  (SystemError(Exception("disk")))        // failed
        Helpers.makeEvent Warning (Custom("coerced", Map.empty))                // warning — not a rejection
    ]
    let result = Harvest.fromEvents 10L (TimeSpan.FromSeconds 1.0) 0L events
    Assert.Equal(10L, result.RecordsRead)
    Assert.Equal(2L,  result.RecordsRejected)
    Assert.Equal(1L,  result.RecordsFailed)
    Assert.Equal(4,   result.Events.Length)
