namespace Xylem.Connectors

open System.IO
open System.Text.Json
open FSharp.Control
open Xylem.Domain

/// <summary>
/// Options controlling how <c>Json.sink</c> writes to a file.
/// </summary>
type JsonLeafOptions = {
    /// <summary>
    /// When <c>true</c>, the JSON output is indented (pretty-printed).
    /// </summary>
    WriteIndented: bool
    /// <summary>
    /// Character encoding for the output file. Defaults to UTF-8 without BOM.
    /// </summary>
    Encoding: System.Text.Encoding
}

module JsonLeafOptions =
    /// <summary>
    /// Default options: no indentation, UTF-8 without BOM.
    /// </summary>
    let Default = {
        WriteIndented = false
        Encoding      = System.Text.UTF8Encoding(encoderShouldEmitUTF8Identifier = false) :> System.Text.Encoding
    }

/// <summary>
/// JSON connectors for reading and writing records.
/// Note: Reading assumes a JSON array of objects or individual JSON objects per line (JSONL).
/// This implementation handles a JSON array of objects.
/// </summary>
module Json =

    /// <summary>
    /// Creates a root that reads a JSON array from the file at <c>path</c>.
    /// </summary>
    let source<'T> (path: string) : Root<'T> = {
        Read = fun () ->
            taskSeq {
                use stream = File.OpenRead(path)
                let items : System.Collections.Generic.IAsyncEnumerable<'T> = 
                    JsonSerializer.DeserializeAsyncEnumerable<'T>(stream)
                for item in items do
                    yield item
            }
    }

    /// <summary>
    /// Creates a leaf with custom options.
    /// </summary>
    let sink<'T> (path: string) (options: JsonLeafOptions) : Leaf<'T> = {
        Write = fun stream -> task {
            use fileStream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None)
            let writerOptions = JsonWriterOptions(Indented = options.WriteIndented)
            use writer = new Utf8JsonWriter(fileStream, writerOptions)
            
            writer.WriteStartArray()
            do! stream |> TaskSeq.iter (fun item -> JsonSerializer.Serialize(writer, item))
            writer.WriteEndArray()
            do! writer.FlushAsync()
        }
    }

    /// <summary>
    /// Creates a leaf with default options.
    /// </summary>
    let sinkDefault<'T> (path: string) : Leaf<'T> =
        sink<'T> path JsonLeafOptions.Default
