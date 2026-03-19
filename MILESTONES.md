## Roadmap table

| Milestone | Version | Priority | Dependencies | Outcome |
|---|---:|---:|---|---|
| Core domain model | v1 | High | None | Defines `Source`, `Flow`, `Sink`, `Pipeline`, `ExecutionContext`, and result types |
| Basic pipeline execution engine | v1 | High | Core domain model | Runs a linear pipeline end-to-end |
| Diagnostics model | v1 | High | Core domain model | Captures structured warnings, errors, counts, and timings |
| Error handling strategy | v1 | High | Core domain model, diagnostics model | Supports explicit failure handling and rejected records |
| Batch processing support | v1 | High | Execution engine | Processes data in manageable chunks |
| Basic transforms | v1 | High | Core domain model, execution engine | Provides map/filter/validate/enrich operations |
| File source and sink | v1 | High | Execution engine, error handling strategy | Enables practical real-world ETL scenarios |
| JSON or CSV connector | v1 | Medium | File source/sink | Adds a common interchange format |
| In-memory test connector | v1 | Medium | Core domain model | Simplifies testing and examples |
| Integration and diagnostics tests | v1 | High | All v1 core features | Verifies correctness and observability |
| Public API documentation | v1 | High | Core v1 features | Makes the library usable and understandable |
| Branching and routing | v2 | High | Stable v1 pipeline model | Enables conditional paths and multi-branch flows |
| Sub-pipelines and reusable fragments | v2 | High | Branching and routing | Improves composition and reuse |
| Checkpointing | v2 | High | Execution engine, diagnostics | Enables resumable runs |
| Resume from checkpoint | v2 | High | Checkpointing | Continues interrupted executions |
| Dead-letter improvements | v2 | High | Error handling strategy | Handles rejected or failed items more flexibly |
| Schema and contract validation | v2 | High | Core domain model, diagnostics | Detects schema drift and incompatibilities |
| Retry policy enhancements | v2 | Medium | Execution engine | Adds richer retry control and resilience |
| Lineage tracking | v2 | Medium | Diagnostics model | Records where data came from and how it changed |
| Run history persistence | v2 | Medium | Diagnostics model | Stores past run summaries and traces |
| Database connector | v2 | Medium | Execution engine, error handling | Adds relational source/sink support |
| HTTP/API connector | v2 | Medium | Execution engine | Supports API-based ingestion/export |
| Queue connector | v2 | Medium | Execution engine | Supports event/message-driven ETL |
| Object storage connector | v2 | Medium | Execution engine | Supports cloud file/object workflows |
| Connector authoring guide | v2 | Medium | Connector set | Helps others extend the framework |
| Operational troubleshooting guide | v2 | Medium | Diagnostics, checkpointing, run history | Supports production use and supportability |

---

## Dependency chain view

If you want the roadmap as a more linear dependency flow:

### v1 foundation
1. Core domain model
2. Diagnostics model
3. Execution engine
4. Error handling strategy
5. Batch processing
6. Basic transforms
7. File connectors
8. Test connectors
9. Tests and docs

### v2 expansion
1. Branching and routing
2. Sub-pipelines
3. Checkpointing
4. Resume support
5. Schema/contract validation
6. Retry enhancements
7. Dead-letter improvements
8. Additional connectors
9. Lineage and run history

---

## Priority guide

- **High**: required for a useful and trustworthy release
- **Medium**: important, but can follow after the core is stable

If you want, I can also convert this into:

1. a **Kanban-style backlog**
2. a **quarter-by-quarter release plan**
3. a **developer task breakdown** for each milestone