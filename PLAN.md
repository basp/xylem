# Xylem — Project Plan

> _From root to result._  
> A composable, diagnostic-first ETL library written in idiomatic F#.

---

## Purpose

Xylem is an ETL library that moves data with the same elegance and efficiency that
plants move water.  The xylem tissue is both backbone and lifeblood — this library
aims to be the same for data pipelines.

---

## Guiding principles

1. **Correctness over performance** — a wrong answer delivered fast is still wrong.
2. **Strong diagnostics** — failures should be loud, structured, and traceable.
3. **Idiomatic F#** — discriminated unions, `Result`, computation expressions, and
   pure functions where possible.
4. **Clear extension points** — adding a custom transform or connector should not
   require fighting the framework.
5. **Gradual complexity** — start simple, earn complexity.

---

## Domain vocabulary

| Term | Meaning |
|---|---|
| `Root<'T>` | Produces records of type `'T` |
| `Vessel<'TIn,'TOut>` | Transforms records from one shape to another |
| `Leaf<'T>` | Consumes records of type `'T` |
| `Pipeline` | Wires a Root → Vessel(s) → Leaf together |
| `ExecutionContext` | Carries run-time config: batch size, cancellation token, etc. |
| `Harvest` | The structured outcome of a run: counts, diagnostics, timing |
| `Pulse` | A structured warning, error, or informational record emitted during a run |
| `Ring` | Aggregated diagnostic counts for a single pipeline stage |
| Rejection | A record that failed validation — tracked, not silently dropped |
| Dead-letter | A record that could not be recovered after all retry attempts |

---

## Current state

The repository is **mid-v1** — the core pipeline model, diagnostics, execution
engine, basic transforms, and first connectors are all implemented and tested.

- `src/Xylem/Domain.fs` — core types (`Root`, `Vessel`, `Leaf`, `Pipeline`,
  `ExecutionContext`, `Harvest`), diagnostics model (`Pulse`, `Ring`), vessel
  combinators (`map`, `filter`, `compose`, `validate`, `enrich`, `batch`), and
  pipeline runners (`run`, `runWith`, `runWithContext`).
- `src/Xylem/InMemory.fs` — in-memory root and leaf for tests and examples.
- `src/Xylem/File.fs` — line-oriented file root and leaf with factory
  constructors for testability.
- `src/Xylem/Sandbox.fsx` — script for ad-hoc exploration.
- `src/Xylem.Tests/` — 83 passing tests covering all implemented features.

---

## v1 — Foundation release (current focus)

The goal is a **reliable, composable ETL core** that can run real pipelines with
strong diagnostics and predictable behaviour.

### Build order (dependency-driven)

1. **Core domain model** — `Root`, `Vessel`, `Leaf`, `Pipeline`, `ExecutionContext`, `Harvest`
2. **Diagnostics model** — structured diagnostic pulses, counts (read / accepted / rejected / failed), timing
3. **Error handling strategy** — `Result`-based outcomes, recoverable vs. fatal, rejection path
4. **Basic execution engine** — async linear pipeline run, cancellation, deterministic stage ordering
5. **Basic transforms** — `map`, `filter`, `validate`, `batch`, `enrich`
6. **Batch processing** — configurable batch sizes, chunked execution
7. **In-memory connector** — for tests and examples (no I/O required)
8. **File root and leaf** — practical real-world ETL
9. **JSON / CSV connector** — common interchange formats
10. **Integration and diagnostics tests** — full-pipeline correctness + observability assertions
11. **Public API documentation** — quickstart, pipeline composition, diagnostics, error handling, connector authoring

### v1 success criteria

A user can:
- wire together a real ETL pipeline in under an hour
- understand failures without stepping through code
- add a custom transformation or connector without fighting the framework
- trust the library to fail loudly and clearly when something breaks

### v1 non-goals

Distributed execution, DAG scheduling, parallel fan-out/fan-in, plugin marketplace,
checkpoint/restart, advanced schema inference, UI/dashboard.

---

## v2 — Growth and specialisation (future)

Adds advanced topology, resilience, schema/contract support, observability, and
more connectors once v1 is stable.

Key additions: branching · fan-out/fan-in · sub-pipelines · checkpointing · resume
· schema validation · retry enhancements · dead-letter improvements · lineage
tracking · run history · database / HTTP / queue / object-storage connectors.

See `ROADMAP.md` for the full milestone table.

---

## Conventions

- **Naming** follows the xylem / botany domain where it adds clarity; avoid forced
  metaphors when plain terms are clearer.
- **Tests first** — write a failing test before implementing a feature.
- **No `mutable` unless unavoidable** — prefer immutable pipelines and explicit
  state threading.
- **Async by default** — all execution-facing APIs are `Async<_>` or `Task<_>`.
- **`Result<_,_>` at boundaries** — never throw across public API boundaries.
- Prefer small, focused modules over large files.

---

## Open questions / decisions to revisit

- ~~`Async<_>` vs `Task<_>`~~ → **`Task<_>` throughout.** `task { }` CEs can `let!` both `Task` and `Async` values natively; `async { }` requires `Async.AwaitTask` wrappers for every .NET library call. The friction compounds at connector boundaries, so `Task` is the clear pragmatic choice.
- ~~Computation expression (`pipeline { ... }`) vs plain function composition?~~ → **Function composition (`|>`) first.** Simpler to implement, easier to test, natural to F# developers. A CE can be layered on top later as syntactic sugar once the underlying combinators are stable.
- ~~Connector interface: type class / interface / plain function record?~~ → **Plain function record (Option B).** Nominally typed so `Root`, `Vessel`, and `Leaf` can't be accidentally confused, but pure-data so construction, testing, and composition are trivial. Leaves a clean door open to add `name`/`metadata` fields later without a breaking change.
- ~~How granular should the diagnostics event model be at v1?~~ → **Structured `Pulse` DU, collected into a list on `Harvest`.** Coarse counters + string errors can't answer "which stage rejected what, and why?" without parsing. A DU is idiomatic F#, machine-readable, and extensible without breaking callers. Counters are a simple fold over the event stream. Live streaming (`IObservable<Pulse>`) is a v2 concern.
