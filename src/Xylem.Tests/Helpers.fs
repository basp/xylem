module Helpers

open System
open Xylem
open Xylem.Connectors
open Xylem.Domain

/// <summary>Alias for the in-memory sink — keeps test call-sites short.</summary>
let collectSink<'T> () = InMemory.sink<'T> ()

let makeEvent severity kind =
    { Severity    = severity
      Kind        = kind
      Stage       = None
      RecordIndex = None
      Timestamp   = DateTimeOffset.UtcNow
      Message     = "test event" }

let positiveValidator (x: int) : Result<int, ErrorKind> =
    if x > 0 then Ok x
    else Result.Error (ValidationError("value", "must be positive"))
