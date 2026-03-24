# 🧬 Core Types

The three building blocks of every Xylem Conduit: `Root`, `Vessel`, and `Leaf`.

---

## `Root<'T>`

A `Root<'T>` is the **entry point** of any Xylem Conduit. It produces a
stream of records of type `'T` as an `IAsyncEnumerable<'T>`.

### Type definition

```fsharp
type Root<'T> = {
    Read: unit -> IAsyncEnumerable<'T>
}
```

The `Read` field is a function so that a root can be re-executed —
calling `Read ()` starts a fresh stream each time. The root itself is
an immutable record; any state (e.g. a database cursor) lives *inside*
the closure returned by `Read`.

### Why `IAsyncEnumerable<'T>`?

- **Streaming** — records are produced and consumed one at a time; the
  entire dataset never needs to be in memory at once.
- **Backpressure** — the consumer drives the pace via `MoveNextAsync()`.
- **Cancellation** — `CancellationToken` flows through
  `GetAsyncEnumerator(ct)` naturally.
- **Unbounded sources** — databases, files, queues, and live event
  streams all model cleanly as async sequences.

### Creating a root

Use the `taskSeq { }` computation expression from
`FSharp.Control.TaskSeq` to produce values:

```fsharp
open FSharp.Control

let numbersRoot : Root<int> = {
    Read = fun () ->
        taskSeq {
            yield 1
            yield 2
            yield 3
        }
}
```

### Consuming a root in tests

Use `TaskSeq.toListAsync` to materialize the stream into a plain list:

```fsharp
let items = numbersRoot.Read() |> TaskSeq.toListAsync |> Async.AwaitTask |> Async.RunSynchronously
// items = [1; 2; 3]
```

Or with `task { }`:

```fsharp
let! items = numbersRoot.Read() |> TaskSeq.toListAsync
// items = [1; 2; 3]
```

---

## `Leaf<'T>`

A `Leaf<'T>` is the **exit point** of a Conduit. It consumes an
`IAsyncEnumerable<'T>` stream and returns `Task<unit>` once all records
have been processed.

### Type definition

```fsharp
type Leaf<'T> = {
    Write: IAsyncEnumerable<'T> -> Task<unit>
}
```

### Why pass the whole stream?

Giving the sink the entire `IAsyncEnumerable<'T>` lets it control its
own iteration strategy:

- A **collecting sink** iterates one item at a time.
- A **database sink** can buffer records into batches before committing.
- A **file sink** can open and close the resource around the whole
  stream rather than per-item.

An item-by-item `Write: 'T -> Task<unit>` interface would force the
Conduit engine to drive iteration, removing that flexibility.

### Creating a leaf

The simplest sink is an in-memory collector — useful in tests:

```fsharp
open System.Collections.Generic
open FSharp.Control

let collectSink () =
    let collected = List<'T>()
    let leaf : Leaf<'T> = {
        Write = fun stream -> task {
            do! stream |> TaskSeq.iter (fun item -> collected.Add(item))
        }
    }
    leaf, collected
```

### Connecting a root to a leaf

Use `Conduit.run` to wire a `Root` to a `Leaf`:

```fsharp
do! Conduit.run root leaf
```

`Conduit.run` simply passes the root stream to the leaf's `Write`
function:

```fsharp
let run (root: Root<'T>) (leaf: Leaf<'T>) : Task<unit> =
    leaf.Write(root.Read())
```

---

## `Vessel<'TIn,'TOut>`

A `Vessel<'TIn,'TOut>` sits **between** a `Root` and a `Leaf`. It
transforms an `IAsyncEnumerable<'TIn>` into an
`IAsyncEnumerable<'TOut>` — lazily, without materializing the stream.

### Type definition

```fsharp
type Vessel<'TIn, 'TOut> = {
    Transform: IAsyncEnumerable<'TIn> -> IAsyncEnumerable<'TOut>
}
```

> **Vessel vs Conduit:** A `Vessel` defines *what* transformation to apply
> — it is a reusable, composable value. The `Conduit` module defines
> *how* to execute a complete `Root` → `Vessel` → `Leaf` chain. Think of a
> `Vessel` as a recipe and `Conduit.runWithContext` as the kitchen that
> runs it.

### Why a stream-to-stream function?

Passing the whole stream (rather than item-by-item) gives the vessel full
control over its iteration strategy:

- A **map** vessel transforms each item individually.
- A **filter** vessel skips items that don't match a predicate.
- A **batch** vessel can group items into chunks before emitting.
- A **window** vessel (future) can look ahead or behind.

All of these are impossible with an item-by-item `'TIn -> 'TOut`
signature.

### Built-in vessel combinators

```fsharp
// Transform every item
let doubled : Vessel<int, int> = Vessel.map (fun x -> x * 2)

// Keep only matching items
let evens : Vessel<int, int> = Vessel.filter (fun x -> x % 2 = 0)

// Group items into arrays of at most N
let inPairsOf3 : Vessel<int, int[]> = Vessel.batch 3
```

### Composing vessels

Two vessels can be composed into one with `Vessel.compose` (or the `>>>` operator):

```fsharp
let doubledEvens : Vessel<int, int> =
    Vessel.map (fun x -> x * 2) >>> Vessel.filter (fun x -> x % 2 = 0)
```

Composition is lazy — no work happens until the stream is consumed.

> **Operator scope:** `>>>` is defined inside `module Vessel` and is
> available when that module is open. When calling from a context where
> the module is not open, use `Vessel.compose` directly:
>
> ```fsharp
> let vessel = Vessel.compose (Vessel.map (fun x -> x * 2)) (Vessel.filter (fun x -> x % 2 = 0))
> ```

### Connecting root, vessel, and leaf

Use `Conduit.runWith` to wire all three together:

```fsharp
do! Conduit.runWith root vessel leaf
```

`Conduit.runWith` threads the stream through the vessel before handing
it to the leaf:

```fsharp
let runWith (root: Root<'TIn>) (vessel: Vessel<'TIn,'TOut>) (leaf: Leaf<'TOut>) : Task<unit> =
    leaf.Write(vessel.Transform(root.Read()))
```
