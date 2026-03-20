module EmitThreadSafetyTests

open System.Threading.Tasks
open Xunit
open Xylem
open Xylem.Domain

[<Fact>]
let ``Concurrent Emit calls preserve all events`` () = task {
    let ctx = ExecutionContext.``default`` ()
    let count = 1000

    let tasks =
        [| for i in 0 .. count - 1 ->
            Task.Run(fun () ->
                ctx.Emit (Helpers.makeEvent Info (Custom($"evt-{i}", Map.empty)))) |]

    do! Task.WhenAll(tasks)

    let events = ctx.ReadEvents()
    Assert.Equal(count, events.Length)
}

[<Fact>]
let ``ReadEvents returns snapshot not affected by later Emit`` () =
    let ctx = ExecutionContext.``default`` ()
    ctx.Emit(Helpers.makeEvent Info (Custom("first", Map.empty)))

    let snapshot = ctx.ReadEvents()

    ctx.Emit(Helpers.makeEvent Warning (Custom("second", Map.empty)))

    Assert.Single(snapshot) |> ignore
    Assert.Equal(2, ctx.ReadEvents().Length)
