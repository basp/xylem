module ExecutionContextTests

open Xunit
open Xylem
open Xylem.Biome

[<Fact>]
let ``ExecutionContext default starts with no events`` () =
    let ctx = ExecutionContext.``default`` ()
    Assert.Empty(ctx.ReadEvents())

[<Fact>]
let ``ExecutionContext Emit adds an event that ReadEvents returns`` () =
    let ctx   = ExecutionContext.``default`` ()
    let event = Helpers.makeEvent Warning (Custom("ping", Map.empty))
    ctx.Emit(event)
    let events = ctx.ReadEvents()
    Assert.Single(events) |> ignore
    Assert.Equal(Warning, events[0].Severity)

[<Fact>]
let ``ExecutionContext Emit accumulates multiple events in order`` () =
    let ctx = ExecutionContext.``default`` ()
    ctx.Emit(Helpers.makeEvent Info    (Custom("a", Map.empty)))
    ctx.Emit(Helpers.makeEvent Warning (Custom("b", Map.empty)))
    ctx.Emit(Helpers.makeEvent Error   (Custom("c", Map.empty)))
    let events = ctx.ReadEvents()
    Assert.Equal(3, events.Length)
    Assert.Equal<Severity list>([Info; Warning; Error], events |> List.map _.Severity)
