namespace Xylem.Tests

open System
open System.IO
open System.Text.Json
open Xylem
open Xylem.Domain
open Xylem.Connectors
open Xunit
open FSharp.Control

module ResilienceTests =

    type Person = { Id: int; Name: string }

    [<Fact>]
    let ``File connector handles locked file during write`` () = task {
        let path = Path.GetTempFileName()
        try
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
            
            // Reading should eventually fail. It might be wrapped in AggregateException depending on how TaskSeq iterates.
            let! ex = Assert.ThrowsAnyAsync<Exception>(fun () -> 
                root.Read() |> TaskSeq.toListAsync :> System.Threading.Tasks.Task)
            
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
        
        let! harvest = Pipeline.runWithContext ctx root (Vessel.transmute id) leaf
        
        Assert.Equal(int64 count, harvest.RecordsRead)
        Assert.True(harvest.Throughput > 0.0, $"Throughput was {harvest.Throughput}")
        Assert.True(harvest.PeakMemoryBytes > 0L, $"Peak memory was {harvest.PeakMemoryBytes}")
        Assert.True(harvest.Duration > TimeSpan.Zero)
    }

    [<Fact>]
    let ``Pipeline retry policy works and emits pulses`` () = task {
        let mutable calls = 0
        let failingRoot : Root<int> = {
            Read = fun () -> taskSeq {
                calls <- calls + 1
                if calls < 3 then
                    failwith "Transient error"
                yield 1
                yield 2
            }
        }
        
        let leaf, _ = InMemory.sink<int> ()
        let ctx = ExecutionContext.``default`` () |> ExecutionContext.withRetryPolicy (FixedDelay (3, TimeSpan.FromMilliseconds 10.0))
        
        let! harvest = Pipeline.runWithContext ctx failingRoot (Vessel.transmute id) leaf
        
        Assert.Equal(3, calls)
        Assert.Equal(2L, harvest.RecordsRead)
        Assert.Equal(2L, harvest.RecordsAccepted)
        
        let retryEvents = harvest.Events |> List.filter (fun e -> match e.Kind with ErrorKind.RetryError _ -> true | _ -> false)
        Assert.Equal(2, retryEvents.Length)
        Assert.All(retryEvents, fun e -> Assert.Equal(Severity.Warning, e.Severity))
    }
