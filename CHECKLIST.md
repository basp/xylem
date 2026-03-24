# Xylem — Progress Checklist

> Track implementation progress against the v1 and v2 roadmap.

---

## v1 — Foundation release

### 1. Core domain model
- [x] Define `Root<'T>`
- [x] Define `Vessel<'TIn, 'TOut>`
- [x] Define `Leaf<'T>`
- [x] Define `Pipeline`
- [x] Define `ExecutionContext`
- [x] Define `Harvest`

### 2. Diagnostics model
- [x] Define `Pulse` record type
- [x] Structured warnings and errors
- [x] Execution timing
- [x] Counts: read / accepted / rejected / failed
- [x] Per-stage summaries (`Ring`)

### 3. Error handling strategy
- [x] `Result`-based outcomes throughout public API
- [x] Recoverable vs fatal error distinction
- [x] Rejection path for invalid records
- [x] Exception capture with context

### 4. Basic execution engine
- [x] Async linear pipeline run (`Task<_>`)
- [x] Cancellation support
- [x] Basic retry policy
- [x] Deterministic stage ordering

### 5. Basic transforms
- [x] `map`
- [x] `filter`
- [x] `validate`
- [x] `batch`
- [x] `enrich`

### 6. Batch processing
- [x] Configurable batch sizes
- [x] Chunked execution

### 7. In-memory connector
- [x] In-memory root
- [x] In-memory leaf

### 8. File connectors
- [x] File root
- [x] File leaf

### 9. JSON connector
- [x] JSON connector

### 10. Integration and diagnostics tests
- [x] Pure function coverage for transforms
- [x] Full pipeline integration test
- [x] Diagnostics assertions
- [x] Failure-path tests

### 11. Public API documentation
- [x] Quickstart guide
- [x] Pipeline composition guide
- [x] Diagnostics guide
- [x] Error handling guide
- [x] Connector authoring guide

---

## v2 — Growth and specialisation

### 1. Advanced pipeline topology
- [ ] Branching
- [ ] Fan-out
- [ ] Fan-in
- [ ] Conditional routing
- [ ] Sub-pipelines
- [ ] Reusable pipeline fragments

### 2. Resilience features
- [ ] Checkpointing
- [ ] Resume from checkpoint
- [ ] Idempotent execution support
- [ ] Richer retry policies
- [ ] Dead-letter handling improvements
- [ ] Compensation hooks for sinks

### 3. Schema and contract support
- [ ] Schema validation
- [ ] Schema versioning
- [ ] Source-to-target shape mapping
- [ ] Compatibility checks
- [ ] Contract-aware transforms

### 4. Performance-aware options
- [ ] Streaming/batch hybrid execution
- [ ] Configurable buffering
- [ ] Safe parallelism
- [ ] Backpressure-aware processing

### 5. Observability enhancements
- [ ] Correlation IDs throughout runs
- [ ] Richer execution trace
- [ ] Lineage tracking
- [ ] Structured run history
- [ ] Exportable diagnostics
- [ ] Summary reports

### 6. Connector expansion
- [ ] Database reader/writer
- [ ] CSV / fixed file reader
- [ ] HTTP / API connector
- [ ] Message queue connector
- [ ] Object storage connector
- [ ] Additional formats (Parquet, XML, …)

### 7. Composition and extensibility
- [ ] Custom stage libraries
- [ ] Shared transform packs
- [ ] Plugin-style connector registration
- [ ] Pipeline templates and presets

### 8. Documentation maturity
- [ ] Advanced routing guide
- [ ] Checkpointing guide
- [ ] Connector authoring deep dive
- [ ] Operational troubleshooting guide
- [ ] Performance and tuning notes
