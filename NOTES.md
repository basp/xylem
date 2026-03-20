## Notes
* Reconsider the `Emit` signature, for example use `DiagnosticEvent -> Task<unit>` instead.
* Reconsider the `Emit` implementation (in the `create` function) and the implications of it currently not being thread-safe.
* `validate` and `enrich` share nearly identical structure — only the type signature and message differ. If a third ctx-aware combinator emerges, it's worth extracting the shared iteration into a private helper.