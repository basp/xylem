## Roadmap goals
The roadmap is optimized for:
 
- **correctness first**
- **strong diagnostics**
- **idiomatic F#**
- **clear extension points**
- **gradual complexity**

---

# v1 — Foundation release

## Objective
Deliver a **reliable, composable ETL core** that can run real pipelines with strong diagnostics and predictable behavior.

## Core outcomes
By the end of v1, users should be able to:

- define a typed pipeline
- read from a source
- transform records
- validate and reject bad inputs
- write to a sink
- inspect detailed execution results
- handle failures without losing visibility

## Scope

### 1. Core pipeline model
- `Root<'T>`
- `Vessel<'TIn, 'TOut>`
- `Leaf<'T>`
- `Pipeline`
- `Harvest`
- `ExecutionContext`

### 2. Basic transformations
- map
- filter
- validate
- batch
- enrich


### 3. Diagnostics
- structured diagnostic pulses
- warnings and errors
- execution timing
- counts for read / accepted / rejected / failed items
- per-stage summaries

### 4. Error handling
- explicit `Result`-based outcomes
- recoverable vs fatal errors
- rejection path for invalid records
- exception capture with context

### 5. Execution engine
- async pipeline execution
- cancellation support
- basic retry policy
- configurable batch sizes
- deterministic stage ordering

### 6. First connectors
Keep this small and useful:
- file root
- file leaf
- JSON or CSV connector
- one simple in-memory connector for tests

### 7. Testability
- pure function coverage for transformations
- integration tests for a full pipeline run
- diagnostics assertions
- failure-path tests

---

## v1 non-goals
To keep the first release focused, avoid:

- distributed execution
- DAG scheduling
- parallel fan-out/fan-in complexity
- plugin marketplace
- checkpoint/restart from arbitrary positions
- advanced schema inference
- UI/dashboard integration

---

## v1 deliverables

### Public API
- minimal, coherent, documented
- stable enough to build production pipelines
- easy to compose in F#

### Developer experience
- sensible defaults
- good error messages
- clear naming aligned to the xylem theme
- examples and templates

### Documentation
- quickstart
- pipeline composition guide
- diagnostics guide
- error handling guide
- connector authoring guide

---

## v1 success criteria
v1 is successful if a user can:

1. wire together a real ETL pipeline in under an hour
2. understand failures without stepping through code
3. add a custom transformation or connector without fighting the framework
4. trust the library to fail loudly and clearly when something breaks

---

# v2 — Growth and specialization release

## Objective
Expand the core into a more complete ETL framework with advanced routing, resiliency, reuse, and operational features.

## Core outcomes
By the end of v2, users should be able to:

- build multi-branch pipelines
- checkpoint and resume runs
- reuse pipeline components across jobs
- handle more complex data movement patterns
- scale execution patterns more confidently
- observe runs in more detail

---

## Scope

### 1. Advanced pipeline topology
- branching
- fan-out
- fan-in
- conditional routing
- sub-pipelines
- reusable pipeline fragments

### 2. Resilience features
- checkpointing
- resume from checkpoint
- idempotent execution support
- richer retry policies
- dead-letter handling improvements
- compensation hooks for sinks

### 3. Schema and contract support
- schema validation
- schema versioning
- mapping between source and target shapes
- compatibility checks
- contract-aware transforms

### 4. Performance-aware options
Even though performance is not the primary goal, v2 can offer:
- streaming/batch hybrid execution
- configurable buffering
- parallelism where safe
- backpressure-aware processing

### 5. Observability enhancements
- correlation IDs throughout the run
- richer execution trace
- lineage tracking
- structured run history
- exportable diagnostics
- summary reports

### 6. Connector expansion
Add more production-grade adapters:
- database reader/writer
- HTTP/API connector
- message queue connector
- object storage connector
- additional formats like Parquet or XML if needed

### 7. Composition and extensibility
- custom stage libraries
- shared transform packs
- plugin-style connector registration
- pipeline templates and presets

---

## v2 non-goals
Still avoid overbuilding too early:

- a full visual pipeline designer
- complex distributed orchestration across clusters
- heavyweight workflow engine semantics
- deeply opinionated hosting platform assumptions

---

## v2 deliverables

### Operational maturity
- checkpoint metadata model
- restart/resume utilities
- run history persistence
- better failure classification
- replayable runs

### API maturity
- stable abstractions from v1
- richer builder/computation-expression support
- clearer extension surfaces
- fewer special cases in user code

### Documentation maturity
- advanced routing guide
- checkpointing guide
- connector authoring deep dive
- operational troubleshooting guide
- performance and tuning notes

---

# Suggested sequencing

## Phase 1: v1.0 core
- core types
- simple execution engine
- diagnostics
- file-based connectors
- tests and docs

## Phase 2: v1.1 hardening
- polish diagnostics
- improve error messages
- add more examples
- tighten edge cases

## Phase 3: v2.0 expansion
- branching and sub-pipelines
- checkpointing
- contract/schema support
- new connectors

## Phase 4: v2.1 operational polish
- lineage reporting
- run history
- resume improvements
- retry/compensation refinements

---

# Recommended release principle

A good rule for this project:

- **v1 = trustworthy core**
- **v2 = expressive platform**

That keeps the first version small enough to finish, while making room for the framework to grow without becoming a tangle of special cases. A very respectable fate for an ETL library — not every pipeline gets to be a majestic tree.

If you want, I can turn this into a **table with milestones, priorities, and dependencies** next.