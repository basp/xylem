module JsonConnectorTests

open System
open System.IO
open System.Text.Json
open FSharp.Control
open Xunit
open Xylem
open Xylem.Biome
open Xylem.Connectors

type Person = {
    Id: int
    Name: string
}

[<Fact>]
let ``Json source reads objects from a JSON array file`` () = task {
    let path = Path.GetTempFileName()
    let data = [
        { Id = 1; Name = "Alice" }
        { Id = 2; Name = "Bob" }
    ]
    let json = JsonSerializer.Serialize(data)
    File.WriteAllText(path, json)

    try
        let root = Json.source<Person> path
        let! result = root.Read() |> TaskSeq.toListAsync

        Assert.Equal(2, result.Length)
        Assert.Equal("Alice", result[0].Name)
        Assert.Equal("Bob", result[1].Name)
    finally
        if File.Exists(path) then File.Delete(path)
}

[<Fact>]
let ``Json sink writes objects as a JSON array to a file`` () = task {
    let path = Path.GetTempFileName()
    let data = taskSeq {
        yield { Id = 1; Name = "Alice" }
        yield { Id = 2; Name = "Bob" }
    }

    try
        let leaf = Json.sinkDefault<Person> path
        do! leaf.Write(data)

        let json = File.ReadAllText(path)
        let result = JsonSerializer.Deserialize<Person list>(json)

        Assert.Equal(2, result.Length)
        Assert.Equal("Alice", result[0].Name)
        Assert.Equal("Bob", result[1].Name)
    finally
        if File.Exists(path) then File.Delete(path)
}

[<Fact>]
let ``Json sink supports WriteIndented option`` () = task {
    let path = Path.GetTempFileName()
    let data = taskSeq { yield { Id = 1; Name = "Alice" } }

    try
        let options = { JsonLeafOptions.Default with WriteIndented = true }
        let leaf = Json.sink<Person> path options
        do! leaf.Write(data)

        let json = File.ReadAllText(path)
        // Simple check for indentation (look for newline)
        Assert.Contains("\n", json.Replace("\r\n", "\n"))
    finally
        if File.Exists(path) then File.Delete(path)
}

[<Fact>]
let ``Full conduit with Json source and sink`` () = task {
    let sourcePath = Path.GetTempFileName()
    let sinkPath = Path.GetTempFileName()
    let data = [ { Id = 1; Name = "Alice" }; { Id = 2; Name = "Bob" } ]
    File.WriteAllText(sourcePath, JsonSerializer.Serialize(data))

    try
        let root = Json.source<Person> sourcePath
        let vessel = Vessel.map (fun p -> { p with Name = p.Name.ToUpper() })
        let leaf = Json.sinkDefault<Person> sinkPath

        do! Conduit.runWith root vessel leaf

        let resultJson = File.ReadAllText(sinkPath)
        let result = JsonSerializer.Deserialize<Person list>(resultJson)

        Assert.Equal(2, result.Length)
        Assert.Equal("ALICE", result[0].Name)
        Assert.Equal("BOB", result[1].Name)
    finally
        if File.Exists(sourcePath) then File.Delete(sourcePath)
        if File.Exists(sinkPath) then File.Delete(sinkPath)
}

// ---------------------------------------------------------------------------
// Json.sourceFrom
// ---------------------------------------------------------------------------

[<Fact>]
let ``Json sourceFrom reads objects from a MemoryStream`` () = task {
    let data = [ { Id = 1; Name = "Alice" }; { Id = 2; Name = "Bob" } ]
    let bytes = JsonSerializer.SerializeToUtf8Bytes(data)
    let root = Json.sourceFrom<Person> (fun () -> new MemoryStream(bytes) :> Stream)

    let! result = root.Read() |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.Equal("Alice", result[0].Name)
    Assert.Equal("Bob", result[1].Name)
}

[<Fact>]
let ``Json sourceFrom calls factory on each Read`` () = task {
    let data = [ { Id = 1; Name = "Alice" } ]
    let bytes = JsonSerializer.SerializeToUtf8Bytes(data)
    let mutable callCount = 0
    let root = Json.sourceFrom<Person> (fun () ->
        callCount <- callCount + 1
        new MemoryStream(bytes) :> Stream)

    let! first  = root.Read() |> TaskSeq.toListAsync
    let! second = root.Read() |> TaskSeq.toListAsync

    Assert.Equal(2, callCount)
    Assert.Equal<string list>([first[0].Name], [second[0].Name])
}

// ---------------------------------------------------------------------------
// Json.sinkFrom / Json.sinkFromDefault
// ---------------------------------------------------------------------------

[<Fact>]
let ``Json sinkFrom writes objects as a JSON array to a MemoryStream`` () = task {
    use ms = new MemoryStream()
    let leaf = Json.sinkFrom<Person> (fun () -> ms :> Stream) JsonLeafOptions.Default
    do! leaf.Write(taskSeq { yield { Id = 1; Name = "Alice" }; yield { Id = 2; Name = "Bob" } })

    let json = System.Text.Encoding.UTF8.GetString(ms.ToArray())
    let result = JsonSerializer.Deserialize<Person list>(json)

    Assert.Equal(2, result.Length)
    Assert.Equal("Alice", result[0].Name)
}

[<Fact>]
let ``Json sinkFrom respects WriteIndented option`` () = task {
    use ms = new MemoryStream()
    let options = { JsonLeafOptions.Default with WriteIndented = true }
    let leaf = Json.sinkFrom<Person> (fun () -> ms :> Stream) options
    do! leaf.Write(taskSeq { yield { Id = 1; Name = "Alice" } })

    let json = System.Text.Encoding.UTF8.GetString(ms.ToArray())
    Assert.Contains("\n", json.Replace("\r\n", "\n"))
}

[<Fact>]
let ``Json sinkFromDefault writes objects as a JSON array to a MemoryStream`` () = task {
    use ms = new MemoryStream()
    let leaf = Json.sinkFromDefault<Person> (fun () -> ms :> Stream)
    do! leaf.Write(taskSeq { yield { Id = 1; Name = "Alice" } })

    let json = System.Text.Encoding.UTF8.GetString(ms.ToArray())
    let result = JsonSerializer.Deserialize<Person list>(json)

    Assert.Equal(1, result.Length)
    Assert.Equal("Alice", result[0].Name)
}

/// <summary>
/// `MemoryStream` rejects cursor-based access after disposal, but it keeps the
/// written bytes buffered internally, so `ToArray()` can still copy them out
/// for verification.
/// </summary>
/// <remarks>
/// `ToArray()` returns a copy of the bytes written up to the stream's current
/// length, not the underlying capacity. That lets the test assert the full JSON
/// payload even after `Write` has disposed of the stream.
/// </remarks>
[<Fact>]
let ``Json sinkFrom disposes the stream but ToArray still works`` () = task {
    use ms = new MemoryStream()
    let leaf = Json.sinkFromDefault<Person> (fun () -> ms :> Stream)
    do! leaf.Write(taskSeq {
        yield { Id = 1; Name = "Alice" }
        yield { Id = 2; Name = "Bob" }
    })

    Assert.Throws<ObjectDisposedException>(fun () -> ms.Position <- 0L) |> ignore

    let json = System.Text.Encoding.UTF8.GetString(ms.ToArray())
    let result = JsonSerializer.Deserialize<Person list>(json)

    Assert.Equal(2, result.Length)
    Assert.Equal("Alice", result[0].Name)
    Assert.Equal("Bob", result[1].Name)
}

[<Fact>]
let ``Json sourceFrom and sinkFrom round-trip`` () = task {
    use ms = new MemoryStream()
    let sink = Json.sinkFromDefault<Person> (fun () -> ms :> Stream)
    do! sink.Write(taskSeq {
        yield { Id = 1; Name = "Alice" }
        yield { Id = 2; Name = "Bob" }
    })

    let bytes = ms.ToArray()
    let source = Json.sourceFrom<Person> (fun () -> new MemoryStream(bytes) :> Stream)
    let! result = source.Read() |> TaskSeq.toListAsync

    Assert.Equal(2, result.Length)
    Assert.Equal("Alice", result[0].Name)
    Assert.Equal("Bob", result[1].Name)
}

// ---------------------------------------------------------------------------
// Json source — existing tests
// ---------------------------------------------------------------------------

[<Fact>]
let ``Json source fails with JsonException when given JSON Lines instead of an array`` () = task {
    let path = Path.GetTempFileName()
    // JSONL (JSON Lines) format - two separate JSON objects on separate lines
    let jsonl = "{\"Id\":1, \"Name\":\"Alice\"}\n{\"Id\":2, \"Name\":\"Bob\"}"
    File.WriteAllText(path, jsonl)

    try
        let root = Json.source<Person> path
        let! ex = Assert.ThrowsAsync<AggregateException>(fun () -> root.Read() |> TaskSeq.toListAsync :> System.Threading.Tasks.Task)
        Assert.IsType<JsonException>(ex.InnerException) |> ignore
        ()
    finally
        if File.Exists(path) then File.Delete(path)
}
