# Connector Authoring Guide

How to write custom connectors for Xylem — sources, sinks, and the patterns that keep them testable.

---

## What is a connector?

A connector is a pair of functions that bridge Xylem's pipeline model
and an external system — a file, a database, an HTTP API, a message
queue, or anything else. Connectors come in two flavours:

| Role | Xylem type | Job |
|------|------------|-----|
| **Source** | `Root<'T>` | Produce a stream of records |
| **Sink** | `Leaf<'T>` | Consume a stream of records |

A single module can expose both (like `File`), or just one (like a
read-only API client). Parsing, validation, and enrichment belong in
a `Vessel`, not in the connector.

---

## Anatomy of a `Root<'T>`

```fsharp
type Root<'T> = {
    Read: unit -> IAsyncEnumerable<'T>
}
```

Every call to `Read ()` must start a **fresh, independent stream**. The
root itself is an immutable record — any mutable state (cursors, file
handles, connections) lives inside the closure returned by `Read`.

### Minimal source

```fsharp
open FSharp.Control
open Xylem.Domain

let mySource : Root<int> = {
    Read = fun () ->
        taskSeq {
            yield 1
            yield 2
            yield 3
        }
}
```

### Real-world source with resource management

```fsharp
let sourceFrom (readerFactory: unit -> TextReader) : Root<string> = {
    Read = fun () ->
        taskSeq {
            use reader = readerFactory ()
            let mutable line = reader.ReadLine()
            while not (isNull line) do
                yield line
                line <- reader.ReadLine()
        }
}
```

Key points:

- **`use` inside `taskSeq`** — the reader is disposed when the consumer
  finishes iterating (or cancels). No manual `try`/`finally` needed.
- **Factory, not instance** — accepting a factory function rather than a
  `TextReader` directly means each `Read ()` call gets a fresh reader.
  This is what makes the root re-entrant.

---

## Anatomy of a `Leaf<'T>`

```fsharp
type Leaf<'T> = {
    Write: IAsyncEnumerable<'T> -> Task<unit>
}
```

The leaf receives the entire stream and controls its own iteration
strategy. It returns `Task<unit>` when all records have been written.

### Minimal sink

```fsharp
let mySink (output: ResizeArray<'T>) : Leaf<'T> = {
    Write = fun stream -> task {
        do! stream |> TaskSeq.iter output.Add
    }
}
```

### Real-world sink with resource management

```fsharp
let sinkFrom (writerFactory: unit -> TextWriter) : Leaf<string> = {
    Write = fun stream -> task {
        use writer = writerFactory ()
        do! stream |> TaskSeq.iter writer.WriteLine
    }
}
```

Key points:

- **`use` inside `task`** — the writer is created once, used for the
  full stream, and disposed when done.
- **Factory, not instance** — same rationale as for sources. The caller
  decides *what* to open; the connector decides *how* to write.

---

## Module layout

Follow the conventions set by the built-in connectors. A well-structured
connector module looks like this:

```fsharp
namespace Xylem.Connectors

open System.IO
open System.Text
open FSharp.Control
open Xylem.Domain

/// Options controlling the sink.
type MyLeafOptions = {
    BufferSize: int
    Encoding:   Encoding
}

module MyLeafOptions =
    let Default = {
        BufferSize = 8192
        Encoding   = UTF8Encoding(false) :> Encoding
    }

module MyConnector =

    // -- Sources --------------------------------------------------------

    /// Testable source: accepts a factory.
    let sourceFrom (factory: unit -> SomeReader) : Root<MyRecord> = {
        Read = fun () -> taskSeq { (* ... *) }
    }

    /// Convenience: opens a real resource by path.
    let source (path: string) : Root<MyRecord> =
        sourceFrom (fun () -> openReader path)

    // -- Sinks ----------------------------------------------------------

    /// Testable sink: accepts a factory.
    let sinkFrom (factory: unit -> SomeWriter) : Leaf<MyRecord> = {
        Write = fun stream -> task { (* ... *) }
    }

    /// Configurable sink: accepts path and options.
    let sink (path: string) (options: MyLeafOptions) : Leaf<MyRecord> =
        sinkFrom (fun () -> openWriter path options)

    /// Convenience: uses default options.
    let sinkDefault (path: string) : Leaf<MyRecord> =
        sink path MyLeafOptions.Default
```

The pattern is **three layers deep**:

1. **`sourceFrom` / `sinkFrom`** — testable core. Accepts a factory so
   tests can inject `StringReader`, `StringWriter`, or mocks.
2. **`source` / `sink`** — configurable. Accepts a path or connection
   string plus an options record.
3. **`sinkDefault`** — convenience. Calls `sink` with sensible defaults.

Not every connector needs all three layers. A source that reads from an
API with no options can skip layers 2 and 3.

---

## Options records

When a connector has configurable behaviour, put the settings in a
dedicated record type:

```fsharp
type FileLeafOptions = {
    Append:   bool
    Encoding: Encoding
}

module FileLeafOptions =
    let Default = {
        Append   = false
        Encoding = UTF8Encoding(false) :> Encoding
    }
```

Guidelines:

- **Immutable records** — callers create options with `{ Default with Append = true }`.
- **Companion module** — a `Default` value in a module with the same
  name as the type so callers always have a starting point.
- **Domain types** — use .NET types (`Encoding`, `TimeSpan`) rather
  than primitive strings or ints where a richer type exists.

---

## Testability

The factory pattern is the backbone of testable connectors. By accepting
a `unit -> 'Resource` factory rather than a path or connection string,
you let tests supply in-memory substitutes:

```fsharp
// Production: reads from a real file.
let source = File.source "data.txt"

// Test: reads from a string — no file on disk.
let source = File.sourceFrom (fun () -> new StringReader("a\nb\nc"))
```

```fsharp
// Production: writes to a real file.
let sink = File.sinkDefault "output.txt"

// Test: writes to a buffer — no file on disk.
let sw   = new StringWriter()
let sink = File.sinkFrom (fun () -> sw :> TextWriter)
```

### What to test

Every connector should cover at least these scenarios:

| Scenario | Why |
|----------|-----|
| Happy path — all records flow through | Proves the basic contract |
| Empty stream | Edge case: source yields nothing, or sink receives nothing |
| Re-entrance — calling `Read ()` twice | Proves each call starts a fresh, independent stream |
| Resource disposal | Proves `use` bindings clean up (e.g. factory call count) |
| Round-trip with a vessel | Integration: source → vessel → sink |
| Factory error surfaces as Fatal | Proves infrastructure failures are visible in the `Harvest` |

### Example test: re-entrance

```fsharp
[<Fact>]
let ``Read called twice calls factory twice`` () = task {
    let mutable callCount = 0
    let source = MyConnector.sourceFrom (fun () ->
        callCount <- callCount + 1
        createReader ())

    let! first  = source.Read() |> TaskSeq.toListAsync
    let! second = source.Read() |> TaskSeq.toListAsync

    Assert.Equal(2, callCount)
    Assert.Equal<MyRecord list>(first, second)
}
```

### Example test: factory error

```fsharp
[<Fact>]
let ``Factory throwing surfaces as Fatal`` () = task {
    let ctx  = ExecutionContext.``default`` ()
    let root = MyConnector.sourceFrom (fun () -> failwith "cannot connect")
    let vessel = Vessel.map id
    let leaf, _ = InMemory.sink<string> ()

    let! result = Pipeline.runWithContext ctx root vessel leaf

    Assert.Equal(1L, result.RecordsFailed)
    Assert.Equal(Fatal, result.Events[0].Severity)
}
```

---

## Error handling

Connectors do **not** emit diagnostics directly. The pipeline engine
handles that:

- **Infrastructure failures** (e.g. file not found, connection refused)
  — let the exception propagate. `Pipeline.runWithContext` catches it
  and records a `Fatal` pulse in the `Harvest`.
- **Per-record failures** (e.g. a malformed row) — handle these in a
  `Vessel` using `Vessel.validate` or `Vessel.enrich`, not in the
  connector. See the [error handling guide](error-handling.md). For
  the core type definitions (`Root`, `Leaf`, `Vessel`), see
  [core types](core-types.md).

If your source encounters a recoverable error mid-stream (e.g. a
transient network hiccup), you have two options:

1. **Let it throw** — the pipeline's `RetryPolicy` will re-execute from
   the beginning if configured.
2. **Retry internally** — if the source can resume from where it left
   off (e.g. an offset-based API), implement retry logic inside the
   `taskSeq` block. This is source-specific and not something Xylem
   prescribes.

---

## Resource management

Use `use` bindings inside `taskSeq { }` and `task { }` to tie resource
lifetime to stream lifetime:

```fsharp
// Source: reader lives as long as the consumer iterates.
let sourceFrom (factory: unit -> TextReader) : Root<string> = {
    Read = fun () ->
        taskSeq {
            use reader = factory ()
            // ...yield records...
        }
}

// Sink: writer lives for the duration of Write.
let sinkFrom (factory: unit -> TextWriter) : Leaf<string> = {
    Write = fun stream -> task {
        use writer = factory ()
        // ...consume stream...
    }
}
```

This guarantees cleanup even when the consumer cancels mid-stream or an
exception is thrown.

---

## Cancellation

Sources do not need to check a `CancellationToken` explicitly. The
pipeline consumer calls `GetAsyncEnumerator(ct)`, and `taskSeq` respects
the token automatically — a cancelled token interrupts `MoveNextAsync`
and triggers disposal.

If your source performs long-running work between yields (e.g. paging
through an API), you can check the token explicitly:

```fsharp
let pagedSource (client: ApiClient) (ct: CancellationToken) : Root<Item> = {
    Read = fun () ->
        taskSeq {
            let mutable cursor = None
            let mutable hasMore = true
            while hasMore do
                ct.ThrowIfCancellationRequested()
                let! page = client.FetchPage(cursor, ct)
                for item in page.Items do
                    yield item
                cursor  <- page.NextCursor
                hasMore <- page.NextCursor.IsSome
        }
}
```

---

## Batching

A sink that benefits from bulk writes (e.g. a database with batch
inserts) can use `Vessel.batch` upstream to receive arrays:

```fsharp
let batchSink (batchSize: int) (conn: DbConnection) : Leaf<MyRecord[]> = {
    Write = fun stream -> task {
        do! stream |> TaskSeq.iter (fun batch ->
            insertBatch conn batch)
    }
}

// Usage:
let vessel = Vessel.map transform >>> Vessel.batch 500
do! Pipeline.runWith source vessel (batchSink 500 conn)
```

The connector receives `MyRecord[]` chunks and can issue a single INSERT
per chunk. The batching logic itself lives in the vessel, keeping the
sink focused on I/O.

---

## Checklist

Before shipping a connector, verify:

- [ ] `Root.Read` returns a fresh, independent stream each call
- [ ] Resources are disposed via `use` bindings
- [ ] A `sourceFrom` / `sinkFrom` overload exists for testability
- [ ] Tests cover: happy path, empty stream, re-entrance, round-trip
- [ ] Infrastructure errors propagate (not silently swallowed)
- [ ] An options record with a `Default` value exists if needed
- [ ] The module lives in the `Xylem.Connectors` namespace
- [ ] XML doc comments on all public functions and types
