# 🌿 Xylem Documentation

> A composable, diagnostic-first ETL library written in idiomatic F#.

Xylem models data Conduits as three composable pieces: a **Root**
(source), a **Vessel** (transform), and a **Leaf** (sink). Every
Conduit run produces a structured **Harvest** with counts, timing, and
a full diagnostic event stream — even when things go wrong.

---

## 🧭 Getting Started

Start with **Core Types** to understand the three building blocks, then
read **Conduits** to see how they fit together. The remaining
guides cover specific topics as you need them.

---

## 🗺️ Guides

### [Core Types](core-types.md)

The three building blocks of every Conduit: `Root<'T>` (async source
stream), `Vessel<'TIn,'TOut>` (lazy stream transform), and `Leaf<'T>`
(async sink). Covers type definitions, creation patterns, composition
with `>>>`, and how `Conduit.run` / `Conduit.runWith` wire them
together.

### [Conduits](conduits.md)

How `Conduit.runWithContext` executes a full Root → Vessel → Leaf
chain. Covers record counting, wall-clock timing, the guaranteed
`Harvest` on failure, and how unhandled exceptions are captured as
`Fatal` diagnostics rather than faulting the task.

---

### [Diagnostics](diagnostics.md)

Xylem's structured diagnostic model. Defines `Severity` (Info through
Fatal), `ErrorKind` (a pattern-matchable discriminated union),
`Pulse` (a single timestamped event), `Harvest` (the complete Conduit
outcome), and `Ring` (per-stage aggregation). Every failure is data,
not an exception.

### [Execution Context](execution-context.md)

The `ExecutionContext` record that coordinates a Conduit run —
carrying cancellation tokens, batch size, diagnostic emission, and
retry policy. Also covers context-aware vessel combinators:
`Vessel.validate` (check records), `Vessel.enrich` (transform with
possible rejection), and `Vessel.batch` (group into fixed-size arrays).

### [Error Handling](error-handling.md)

The complete error handling story — from per-record rejections
(validation and enrichment) to Conduit-level crashes. Covers severity
levels, choosing the right `ErrorKind`, inspecting `Harvest` counts
and `Pulse` lists, per-stage summaries via `Ring`, and common patterns
like validate-then-enrich Conduits.

### [Retry Policy](retry-policy.md)

How `RetryPolicy` controls automatic retry on Conduit failure.
Covers `NoRetry` (default) and `FixedDelay` configuration, what gets
retried (the entire Conduit from scratch), diagnostic events emitted
during retries, and cancellation behaviour between attempts.

### [Connectors](connectors.md)

The three built-in connectors: `InMemory` (lists and arrays — ideal for
tests), `File` (line-oriented local file I/O), and `Json` (structured
JSON array files). Covers factory constructors for testability,
`FileLeafOptions`, `JsonLeafOptions`, resource lifetime, testing without
I/O, and full Conduit examples.

### [Connector Authoring](connector-authoring.md)

How to write custom connectors — sources and sinks — for any external
system. Covers the `Root<'T>` / `Leaf<'T>` contracts, the three-layer
module pattern (`sourceFrom` / `source` / `sinkDefault`), options
records, testability via factory functions, error handling, resource
management, cancellation, and batching.

### [Design Decisions](design-decisions.md)

Architectural rationale behind key choices: how vessels emit
diagnostics (side-channel vs in-stream `Result`), exception handling
in `runWithContext`, whole-Conduit retry vs per-record retry, `Ring`
as a standalone function vs a `Harvest` field, and why `Emit` is
synchronous.
