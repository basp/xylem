namespace Xylem.Connectors

open System.IO
open System.Text.Json
open FSharp.Control
open Xylem.Biome

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
/// </summary>
/// <remarks>
/// <ul>
/// <li>Reading assumes a JSON array of objects.</li>
/// <li>This implementation handles a single JSON array at the root of the file.</li>
/// </ul>
/// </remarks>
module Json =

    /// <summary>
    /// Creates a root from a <c>Stream</c> factory.
    /// Each call to <c>Read ()</c> invokes <c>streamFactory</c> to get a fresh
    /// readable <c>Stream</c>, deserializes a JSON array from it, and disposes
    /// the stream when enumeration ends. Useful in tests by supplying a
    /// <c>MemoryStream</c> factory.
    /// </summary>
    let sourceFrom<'T> (streamFactory: unit -> Stream) : Root<'T> = {
        Read = fun () ->
            taskSeq {
                use stream = streamFactory ()
                let items : System.Collections.Generic.IAsyncEnumerable<'T> =
                    JsonSerializer.DeserializeAsyncEnumerable<'T>(stream)
                for item in items do
                    yield item
            }
    }

    /// <summary>
    /// Creates a root that reads a JSON array from the file at <c>path</c>.
    /// </summary>
    let source<'T> (path: string) : Root<'T> =
        sourceFrom<'T> (fun () -> File.OpenRead(path) :> Stream)

    /// <summary>
    /// Creates a leaf from a <c>Stream</c> factory and options.
    /// <c>Write</c> invokes <c>streamFactory</c> once, serializes each item into
    /// a JSON array, then disposes the stream. Useful in tests by supplying a
    /// <c>MemoryStream</c> factory.
    /// </summary>
    let sinkFrom<'T> (streamFactory: unit -> Stream) (options: JsonLeafOptions) : Leaf<'T> = {
        Write = fun stream -> task {
            use fileStream = streamFactory ()
            let writerOptions = JsonWriterOptions(Indented = options.WriteIndented)
            use writer = new Utf8JsonWriter(fileStream, writerOptions)

            writer.WriteStartArray()
            do! stream |> TaskSeq.iter (fun item -> 
                JsonSerializer.Serialize(writer, item)
                writer.Flush())
            writer.WriteEndArray()
            do! writer.FlushAsync()
        }
    }

    /// <summary>
    /// Creates a leaf from a <c>Stream</c> factory using default options.
    /// </summary>
    let sinkFromDefault<'T> (streamFactory: unit -> Stream) : Leaf<'T> =
        sinkFrom<'T> streamFactory JsonLeafOptions.Default

    /// <summary>
    /// Creates a leaf with custom options.
    /// </summary>
    let sink<'T> (path: string) (options: JsonLeafOptions) : Leaf<'T> =
        sinkFrom<'T> (fun () -> new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None) :> Stream) options

    /// <summary>
    /// Creates a leaf with default options.
    /// </summary>
    let sinkDefault<'T> (path: string) : Leaf<'T> =
        sink<'T> path JsonLeafOptions.Default
