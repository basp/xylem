module JsonConnectorTests

open System
open System.IO
open System.Text.Json
open FSharp.Control
open Xunit
open Xylem
open Xylem.Domain
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
        Assert.Equal("Alice", result.[0].Name)
        Assert.Equal("Bob", result.[1].Name)
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
        Assert.Equal("Alice", result.[0].Name)
        Assert.Equal("Bob", result.[1].Name)
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
let ``Full pipeline with Json source and sink`` () = task {
    let sourcePath = Path.GetTempFileName()
    let sinkPath = Path.GetTempFileName()
    let data = [ { Id = 1; Name = "Alice" }; { Id = 2; Name = "Bob" } ]
    File.WriteAllText(sourcePath, JsonSerializer.Serialize(data))

    try
        let root = Json.source<Person> sourcePath
        let vessel = Vessel.map (fun p -> { p with Name = p.Name.ToUpper() })
        let leaf = Json.sinkDefault<Person> sinkPath

        do! Pipeline.runWith root vessel leaf

        let resultJson = File.ReadAllText(sinkPath)
        let result = JsonSerializer.Deserialize<Person list>(resultJson)

        Assert.Equal(2, result.Length)
        Assert.Equal("ALICE", result.[0].Name)
        Assert.Equal("BOB", result.[1].Name)
    finally
        if File.Exists(sourcePath) then File.Delete(sourcePath)
        if File.Exists(sinkPath) then File.Delete(sinkPath)
}
