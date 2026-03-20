# Connectors

> Built-in connectors that ship with Xylem — InMemory for tests and
> File for local file system I/O.

---

A **connector** is a `Root<'T>` or `Leaf<'T>` that ties the pipeline
to a specific data store or transport. The core library ships two
connectors out of the box: `Xylem.Connectors.InMemory` and
`Xylem.Connectors.File`. JSON/CSV, database, and queue connectors are
planned for future releases.

For guidance on writing your own connectors, see the
[connector authoring guide](connector-authoring.md).

---

## `Xylem.Connectors.InMemory`

The in-memory connector requires no I/O and no external dependencies.
It is the go-to choice for tests, examples, and simple one-off
pipelines.

```fsharp
open Xylem.Connectors
```

### `InMemory.source`

Creates a `Root<'T>` from any `seq<'T>`-compatible value — lists,
arrays, and sequences all work:

```fsharp
let root = InMemory.source [1; 2; 3; 4; 5]
let root = InMemory.source [| "a"; "b"; "c" |]
let root = InMemory.source (seq { for i in 1..100 do yield i })
```

Each call to `root.Read()` produces a fresh, independent
`IAsyncEnumerable<'T>` — the root behaves exactly like any other
Xylem root. Multiple runs over the same root are safe as long as
the underlying sequence is re-iterable (lists and arrays always are;
one-shot `seq` expressions are not).

### `InMemory.sink`

Creates a `Leaf<'T>` that accumulates every written item into an
internal buffer. Returns the sink together with a **reader function**
that snapshots the collected items on demand:

```fsharp
let leaf, read = InMemory.sink ()

do! Pipeline.runWith root vessel leaf

let items : int list = read ()   // ["item-2"; "item-4"; ...]
```

Each call to `read ()` returns a fresh `'T list` — the same items, a
different list object. This mirrors the `ExecutionContext.ReadEvents`
pattern and makes call-site assertions straightforward:

```fsharp
Assert.Equal<int list>([2; 4], read ())
```

### Full pipeline example

```fsharp
open Xylem.Connectors

let ctx    = ExecutionContext.``default`` ()
let root = InMemory.source [1; -2; 3; -4; 5]
let vessel = Vessel.validate "check-positive" ctx (fun x ->
    if x > 0 then Ok x
    else Result.Error (ValidationError("value", "must be positive")))
let leaf, read = InMemory.sink ()

let! result = Pipeline.runWithContext ctx root vessel leaf

printfn $"Accepted: %A{read ()}"          // [1; 3; 5]
printfn $"Rejected: %d{result.RecordsRejected}"  // 2
```

### Design decisions — `InMemory`

#### `#seq<'T>` vs `seq<'T>` for the root input

`InMemory.source` accepts `#seq<'T>` (a flexible type) rather than
`seq<'T>`. The difference: with `seq<'T>`, passing a `list` or
`array` boxes it into a plain `seq` before the function is entered —
the static type is lost. With `#seq<'T>`, the compiler accepts any
subtype of `IEnumerable<'T>` without an upcast, keeping the most
specific type at the call site.

In practice the difference is invisible at runtime, but `#seq<'T>` is
the idiomatic F# choice for functions that accept *any* sequence.

#### `unit -> 'T list` reader vs returning the list directly

`InMemory.sink` returns `Leaf<'T> * (unit -> 'T list)` rather than
`Leaf<'T> * 'T list`. If it returned the list directly, the list would
be captured at construction time — before any items have been written —
and would always be empty.

The reader function defers evaluation: each call snapshots the buffer
*at that moment*, which is what tests and post-run inspection actually
need. This is the same deferred-reader pattern used by
`ExecutionContext.ReadEvents`.

#### `'T list` vs `ResizeArray<'T>` in the snapshot

The reader converts the internal `ResizeArray<'T>` to a `'T list` on
every call via `List.ofSeq`. This means:

- **Immutable** — callers can hold onto a snapshot without worrying
  about it changing under them.
- **Idiomatic** — F# assertion helpers and pattern matching work
  naturally on lists.
- **One allocation per read** — acceptable for the test/example use
  case this connector targets; not a concern for production connectors
  where the sink itself owns the write strategy.

---

## `Xylem.Connectors.File`

The file connector reads from and writes to the local file system.
Like all Xylem connectors, it lives in `Xylem.Connectors`:

```fsharp
open Xylem.Connectors
```

### Responsibility boundary — lines only

`File.source` produces `string` lines. `File.sink` consumes `string`
lines. **Parsing and serialisation are not the connector's job** — they
belong in a `Vessel` sitting between root and leaf.

This keeps each piece focused:

```
File.source "input.csv"
  → Vessel.map parseCsvRow       // string → Row
  → Vessel.validate "check" ctx validator
  → Vessel.map formatCsvRow      // Row → string
  → File.sink "output.csv"
```

A connector that bundled its own CSV or JSON parser would duplicate the
format connectors and force every caller to use its specific parser
whether they wanted to or not.

### `File.sourceFrom` — factory constructor

The primary constructor. Accepts a factory function that produces a
`TextReader` and wraps it as a `Root<string>`:

```fsharp
File.sourceFrom : (unit -> TextReader) -> Root<string>
```

Each call to `root.Read()` invokes the factory to obtain a fresh
`TextReader`, yields its lines one at a time, and disposes the reader
when enumeration ends. The factory is called lazily — only when the
first item is pulled from the stream.

This is the overload to use in tests (see *Testing without I/O* below).

### `File.source` — convenience overload

Wraps `sourceFrom` with a `StreamReader` factory for a file path:

```fsharp
let root : Root<string> = File.source "data.csv"
```

Equivalent to:

```fsharp
let root = File.sourceFrom (fun () -> new StreamReader("data.csv"))
```

Each call to `root.Read()` opens a fresh `StreamReader`, streams
lines one at a time, and disposes the reader on completion, cancellation,
or exception. The file is never fully loaded into memory.

Empty lines are yielded as empty strings — they are not skipped. Use
`Vessel.filter (fun line -> line <> "")` to drop them upstream.

If the file does not exist or cannot be opened, the `StreamReader`
constructor throws during the first iteration. The exception is caught
by `Pipeline.runWithContext`, which emits a `Fatal` diagnostic and
returns a well-formed `Harvest`.

### `File.sinkFrom` — factory constructor

The primary constructor. Accepts a factory function that produces a
`TextWriter` and wraps it as a `Leaf<string>`:

```fsharp
File.sinkFrom : (unit -> TextWriter) -> Leaf<string>
```

`Write` invokes the factory once, writes each string as a line via
`TextWriter.WriteLine`, then disposes the writer — whether the stream
completes normally or throws.

This is the overload to use in tests (see *Testing without I/O* below).

### `File.sink` — convenience overloads

Wraps `sinkFrom` with a `StreamWriter` factory for a file path:

```fsharp
// Overwrite (default) — one-arg convenience
let leaf : Leaf<string> = File.sinkDefault "output.csv"

// With explicit options
let leaf = File.sink "output.csv" { FileLeafOptions.Default with Append = true }
```

The default behaviour **overwrites** the file if it already exists —
pipelines are designed to be re-runnable, and appending to a previous
run's output would produce corrupt data. Append is opt-in via
`FileLeafOptions`.

### `FileLeafOptions`

```fsharp
type FileLeafOptions = {
    Append:   bool
    Encoding: System.Text.Encoding
}
```

| Field | Default | Purpose |
|---|---|---|
| `Append` | `false` | When `true`, new lines are appended to an existing file rather than overwriting it |
| `Encoding` | `UTF-8` (no BOM) | Character encoding for the output file |

### Resource lifetime

| Connector | `TextReader`/`TextWriter` opened | Closed |
|---|---|---|
| `File.sourceFrom` | At first `MoveNextAsync()` call | When enumeration ends (completion, cancellation, or exception) |
| `File.sinkFrom` | At the start of `Write(stream)` | When `Write` returns (success or exception) |

Both use `use` bindings so disposal is guaranteed regardless of how the
stream terminates. Multiple calls to `root.Read()` are safe — each
call invokes the factory independently.

### Testing without I/O

Because both primary constructors accept a factory function, tests
substitute `StringReader` and `StringWriter` — standard .NET types —
in place of real file handles. No temp files, no cleanup, no
environment dependencies:

```fsharp
// Source — inject a StringReader
let root = File.sourceFrom (fun () -> new StringReader("alice\nbob\ncarol"))

let! lines = root.Read() |> TaskSeq.toListAsync
// lines = ["alice"; "bob"; "carol"]

// Sink — inject a StringWriter and inspect what was written
let sw   = new StringWriter()
let leaf = File.sinkFrom (fun () -> sw :> TextWriter)

do! leaf.Write(taskSeq { yield "x"; yield "y" })
// sw.ToString() = "x\r\ny\r\n"  (or "x\ny\n" on Unix)
```

The convenience overloads (`File.source path`, `File.sink path`) are
integration-tested separately as thin wrappers over `sourceFrom`/`sinkFrom`.

### Full pipeline example

```fsharp
open Xylem
open Xylem.Connectors
open Xylem.Domain

type Row = { Name: string; Age: int }

let parseLine (line: string) : Result<Row, ErrorKind> =
    match line.Split(',') with
    | [| name; age |] ->
        match System.Int32.TryParse(age) with
        | true, n -> Ok { Name = name.Trim(); Age = n }
        | _       -> Result.Error (ValidationError("Age", "not a valid integer"))
    | _ -> Result.Error (ValidationError("line", "expected 2 comma-separated fields"))

let formatLine (row: Row) : string = $"{row.Name},{row.Age}"

let ctx = ExecutionContext.``default`` ()

let root = File.source "people.csv"
let vessel =
    Vessel.compose
        (Vessel.enrich "parse" ctx parseLine)
        (Vessel.compose
            (Vessel.validate "check-age" ctx (fun row ->
                if row.Age >= 0 then Ok row
                else Result.Error (ValidationError("Age", "must be non-negative"))))
            (Vessel.map formatLine))
let leaf = File.sinkDefault "people-clean.csv"

let! result = Pipeline.runWithContext ctx root vessel leaf

printfn $"Read: %d{result.RecordsRead}  Accepted: %d{result.RecordsAccepted}  Rejected: %d{result.RecordsRejected}"
```

### Design decisions — `File`

#### Lines only, not generic

The alternative would be `File.source<'T>` with a built-in
`string -> 'T` deserialiser parameter. This conflates connector and
format concerns: the connector would need to know about CSV, JSON, or
whatever format the caller chooses.

Keeping connectors as `Root<string>` / `Leaf<string>` means format
handling belongs in a `Vessel`, which is the correct abstraction for
record-level transforms. The upcoming JSON and CSV connectors will
follow the same principle — they will be thin wrappers that compose a
`File.source` with a parsing vessel.

#### Factory functions for testability

Accepting `unit -> TextReader` and `unit -> TextWriter` rather than a
raw path keeps the connector testable without any mocking framework or
file system abstraction layer. `StringReader` and `StringWriter` are
standard .NET types that implement `TextReader` and `TextWriter`
respectively — no new interfaces or dependencies are needed.

The convenience path-based overloads (`File.source`, `File.sink`) are
thin wrappers that supply the `StreamReader`/`StreamWriter` factory.
They carry no logic of their own, so unit tests can focus entirely on
the `sourceFrom`/`sinkFrom` behaviour.

#### Overwrite by default

Append-by-default would silently corrupt output on a rerun.
Overwrite-by-default makes pipelines idempotent and rerunnable without
manual cleanup. Append is opt-in via `FileLeafOptions`.

#### `UTF-8` without BOM

UTF-8 without BOM is the cross-platform default. BOM causes problems
with many Unix tools and some parsers. Callers that need BOM or a
different encoding can supply a custom `Encoding` via `FileLeafOptions`.

#### Context-free connectors

`File.source` and `File.sink` do not accept an `ExecutionContext`. I/O
failures at the connector level (file not found, permission denied) are
fatal and unrecoverable — they are not per-record events. The engine's
`try/catch` in `runWithContext` handles them uniformly, emitting a
`Fatal` diagnostic and returning a well-formed `Harvest`. See
[design decisions — exception handling](design-decisions.md#exception-handling-in-runwithcontext)
for full details.
