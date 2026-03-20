## Notes
* Reconsider the `Emit` signature, for example use `DiagnosticEvent -> Task<unit>` instead.
* Reconsider the `Emit` implementation (in the `create` function) and the implications of it currently not being thread-safe.