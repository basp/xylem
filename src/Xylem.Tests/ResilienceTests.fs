namespace Xylem.Tests

open System
open System.IO
open System.Text.Json
open Xylem
open Xylem.Biome
open Xylem.Connectors
open Xunit
open FSharp.Control

module ResilienceTests =

    type Person = { Id: int; Name: string }

    [<Fact>]
    let ``File connector handles locked file during write`` () = task {
        let path = Path.GetTempFileName()
        try
            // Open and lock the file
            use _ = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
            
            let leaf = File.sinkDefault path
            let root = InMemory.source ["hello"]
            
            // Should fail due to lock
            let! ex = Assert.ThrowsAsync<IOException>(fun () -> leaf.Write(root.Read()))
            Assert.Contains("being used by another process", ex.Message)
        finally
            if File.Exists(path) then File.Delete(path)
    }

    [<Fact>]
    let ``Json connector handles corrupt JSON during read`` () = task {
        let path = Path.GetTempFileName()
        try
            File.WriteAllText(path, "[{\"Id\":1, \"Name\":\"Alice\"}, {corrupt}]")
            
            let root = Json.source<Person> path
            
            // Reading should eventually fail.
            // It might be wrapped in AggregateException depending on how TaskSeq iterates.
            let! ex = Assert.ThrowsAnyAsync<Exception>(fun () -> 
                root.Read()
                |> TaskSeq.toListAsync :> System.Threading.Tasks.Task)
            
            // Recursively check for a JsonException.
            let isJsonEx (e: Exception) =
                match e with
                | :? JsonException -> true
                | :? AggregateException as ae -> ae.InnerExceptions |> Seq.exists (fun ie -> ie :? JsonException)
                | _ -> false

            Assert.True(isJsonEx ex, $"Expected JsonException, but got {ex.GetType().Name}: {ex.Message}")
        finally
            if File.Exists(path) then File.Delete(path)
    }

    [<Fact>]
    let ``Harvest tracks throughput and peak memory`` () = task {
        let count = 5000
        let items = Array.init count (fun i -> { Id = i; Name = $"Person {i}" })
        let root = InMemory.source items
        let leaf, _ = InMemory.sink<Person> ()
        let ctx = ExecutionContext.``default`` ()
        
        let! harvest = Conduit.runWithContext ctx root (Vessel.transmute id) leaf
        
        Assert.Equal(int64 count, harvest.RecordsRead)
        Assert.True(harvest.Throughput > 0.0, $"Throughput was {harvest.Throughput}")
        Assert.True(harvest.PeakMemoryBytes > 0L, $"Peak memory was {harvest.PeakMemoryBytes}")
        Assert.True(harvest.Duration > TimeSpan.Zero)
    }

    [<Fact>]
    let ``Conduit retry policy works and emits pulses`` () = task {
        let mutable calls = 0
        let failingRoot : Root<int> = {
            Read = fun () -> taskSeq {
                calls <- calls + 1
                if calls < 3 then
                    failwith "Transient error"
                yield 1
                yield 2
                yield 3
                yield 4
                yield 5
            }
        }
        
        let leaf, _ = InMemory.sink<int> ()
        let policy = FixedDelay (3, TimeSpan.FromMilliseconds 10.0)
        let ctx =
            ExecutionContext.``default`` ()
            |> ExecutionContext.withRetryPolicy policy
        
        let! harvest = Conduit.runWithContext ctx failingRoot (Vessel.transmute id) leaf
        
        Assert.Equal(3, calls)
        Assert.Equal(5L, harvest.RecordsRead)
        Assert.Equal(5L, harvest.RecordsAccepted)
        
        let retryEvents = harvest.Events |> List.filter (fun e -> match e.Kind with ErrorKind.RetryError _ -> true | _ -> false)
        Assert.Equal(2, retryEvents.Length)
        Assert.All(retryEvents, fun e -> Assert.Equal(Severity.Warning, e.Severity))
    }

    [<Fact>]
    let ``Conduit retries when stream fails mid-iteration`` () = task {
        let mutable calls = 0
        
        let failingRoot : Root<int> = {
            Read = fun () -> taskSeq {
                calls <- calls + 1
                if calls = 1 then
                    yield 1
                    yield 2
                    failwith "Mid-stream transient error"
                else
                    yield 1
                    yield 2
                    yield 3
            }
        }
        
        let leaf, getResults = InMemory.sink<int> ()
        let policy = FixedDelay (1, TimeSpan.FromMilliseconds 10.0)
        let ctx =
            ExecutionContext.``default`` ()
            |> ExecutionContext.withRetryPolicy policy
            
        let! harvest = Conduit.runWithContext ctx failingRoot (Vessel.transmute id) leaf
        
        Assert.Equal(2, calls)
        Assert.Equal(3L, harvest.RecordsRead)
        Assert.Equal(3L, harvest.RecordsAccepted)
        
        // Note: InMemory.sink is idempotent — it clears on each Write call,
        // so it only contains items from the final successful attempt.
        let results = getResults ()
        Assert.Equal<int list>([1; 2; 3], results)
    }

    [<Fact>]
    let ``File sink handles interrupted stream and recovers via retry`` () = task {
        let path = Path.GetTempFileName()
        try
            let mutable calls = 0
            let failingRoot : Root<string> = {
                Read = fun () -> taskSeq {
                    calls <- calls + 1
                    if calls = 1 then
                        yield "line 1"
                        yield "line 2"
                        failwith "Mid-stream failure"
                    else
                        yield "line 1"
                        yield "line 2"
                        yield "line 3"
                }
            }
            
            let leaf = File.sinkDefault path
            let policy = FixedDelay (1, TimeSpan.FromMilliseconds 10.0)
            let ctx =
                ExecutionContext.``default`` ()
                |> ExecutionContext.withRetryPolicy policy
                
            let! _ = Conduit.runWithContext ctx failingRoot (Vessel.transmute id) leaf
            
            Assert.Equal(2, calls)
            let content = File.ReadAllLines(path)
            Assert.Equal<string[]>( [| "line 1"; "line 2"; "line 3" |], content)
        finally
            if File.Exists(path) then File.Delete(path)
    }

    [<Fact>]
    let ``Json sink handles interrupted stream and recovers via retry`` () = task {
        let path = Path.GetTempFileName()
        try
            let mutable calls = 0
            let failingRoot : Root<Person> = {
                Read = fun () -> taskSeq {
                    calls <- calls + 1
                    if calls = 1 then
                        yield { Id = 1; Name = "Alice" }
                        failwith "Mid-stream failure"
                    else
                        yield { Id = 1; Name = "Alice" }
                        yield { Id = 2; Name = "Bob" }
                }
            }
            
            let leaf = Json.sinkDefault<Person> path
            let policy = FixedDelay (1, TimeSpan.FromMilliseconds 10.0)
            let ctx =
                ExecutionContext.``default`` ()
                |> ExecutionContext.withRetryPolicy policy
                
            let! _ = Conduit.runWithContext ctx failingRoot (Vessel.transmute id) leaf
            
            Assert.Equal(2, calls)
            let json = File.ReadAllText(path)
            let items = JsonSerializer.Deserialize<Person[]>(json)
            Assert.Equal(2, items.Length)
            Assert.Equal("Alice", items[0].Name)
            Assert.Equal("Bob", items[1].Name)
        finally
            if File.Exists(path) then File.Delete(path)
    }

    [<Fact>]
    let ``Conduit emits Fatal pulse after all retries exhausted for mid-stream error`` () = task {
        let mutable calls = 0
        let failingRoot : Root<int> = {
            Read = fun () -> taskSeq {
                calls <- calls + 1
                yield 1
                failwith $"Failure in attempt {calls}"
            }
        }
        
        let leaf, _ = InMemory.sink<int> ()
        let policy = FixedDelay (2, TimeSpan.FromMilliseconds 10.0)
        let ctx =
            ExecutionContext.``default`` ()
            |> ExecutionContext.withRetryPolicy policy
            
        let! harvest = Conduit.runWithContext ctx failingRoot (Vessel.transmute id) leaf
        
        Assert.Equal(3, calls) // Initial + 2 retries
        Assert.Equal(1L, harvest.RecordsRead) 
        Assert.Equal(1, harvest.Events |> List.filter (fun e -> e.Severity = Severity.Fatal) |> List.length)
        
        let fatal = harvest.Events |> List.find (fun e -> e.Severity = Severity.Fatal)
        match fatal.Kind with
        | ErrorKind.RetryError (attempt, _) -> Assert.Equal(3, attempt)
        | _ -> failwith "Expected RetryError"
    }
