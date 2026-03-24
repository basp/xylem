# Xylem Guide

> A composable, diagnostic-first ETL library written in idiomatic F#.

The guide is organized into focused documents. Start with core types,
then follow the links as needed.

## Reference

| Document | Description |
|---|---|
| [Core Types](docs/core-types.md) | `Root`, `Leaf`, `Vessel` — the three building blocks |
| [Diagnostics](docs/diagnostics.md) | `Severity`, `ErrorKind`, `Pulse`, `Harvest`, `Ring` |
| [Execution Context](docs/execution-context.md) | `ExecutionContext` + context-aware combinators (`validate`, `enrich`, `batch`) |
| [Running Pipelines](docs/pipelines.md) | `Pipeline.runWithContext`, guaranteed `Harvest`, failure handling |
| [Error Handling](docs/error-handling.md) | Per-record rejections, pipeline-level crashes, inspecting results |
| [Retry Policy](docs/retry-policy.md) | `RetryPolicy` configuration, behaviour, and cumulative retry diagnostics |
| [Connectors](docs/connectors.md) | Built-in `InMemory`, `File`, and `Json` connectors |
| [Connector Authoring](docs/connector-authoring.md) | Writing custom connectors — sources, sinks, and testing patterns |
| [Design Decisions](docs/design-decisions.md) | Architectural rationale — trade-offs and alternatives considered |

