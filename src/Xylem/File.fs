namespace Xylem.Connectors

open System.IO
open System.Text
open FSharp.Control
open Xylem.Biome

/// <summary>
/// Options controlling how <c>File.sink</c> writes to a file.
/// Use <c>FileLeafOptions.Default</c> for the most common case.
/// </summary>
type FileLeafOptions = {
    /// <summary>
    /// When <c>true</c>, lines are appended to an existing file.
    /// When <c>false</c> (the default), the file is overwritten.
    /// </summary>
    Append: bool
    /// <summary>
    /// Character encoding for the output file. Defaults to UTF-8 without BOM.
    /// </summary>
    Encoding: Encoding
}

module FileLeafOptions =

    /// <summary>
    /// Default options: overwrite mode, UTF-8 without BOM.
    /// </summary>
    let Default = {
        Append   = false
        Encoding = UTF8Encoding(encoderShouldEmitUTF8Identifier = false) :> Encoding
    }

/// <summary>
/// Line-oriented file connectors. Produces and consumes <c>string</c> lines.
/// Parsing and serialization belong in a <c>Vessel</c>, not in the connector.
/// </summary>
module File =

    /// <summary>
    /// Creates a <c>Root&lt;string&gt;</c> from a <c>TextReader</c> factory.
    /// Each call to <c>Read ()</c> invokes <c>readerFactory</c> to get a fresh
    /// <c>TextReader</c>, yields its lines one at a time, and disposes the reader
    /// when enumeration ends. Use this overload in tests by supplying a
    /// <c>StringReader</c> factory.
    /// </summary>
    let sourceFrom (readerFactory: unit -> TextReader) : Root<string> = {
        Read = fun () ->
            taskSeq {
                use reader = readerFactory ()
                let mutable line = reader.ReadLine()
                while not (isNull line) do
                    yield line
                    line <- reader.ReadLine()
            }
    }

    /// <summary>
    /// Creates a <c>Root&lt;string&gt;</c> that reads lines from the file at
    /// <c>path</c>. Each call to <c>Read ()</c> opens a fresh <c>StreamReader</c>.
    /// </summary>
    let source (path: string) : Root<string> =
        sourceFrom (fun () -> new StreamReader(path) :> TextReader)

    /// <summary>
    /// Creates a <c>Leaf&lt;string&gt;</c> from a <c>TextWriter</c> factory.
    /// <c>Write</c> invokes <c>writerFactory</c> once, writes each string as a
    /// line via <c>TextWriter.WriteLine</c>, then disposes the writer. Use this
    /// overload in tests by supplying a <c>StringWriter</c> factory.
    /// </summary>
    let sinkFrom (writerFactory: unit -> TextWriter) : Leaf<string> = {
        Write = fun stream -> task {
            use writer = writerFactory ()
            do! stream |> TaskSeq.iter writer.WriteLine
        }
    }

    /// <summary>
    /// Creates a <c>Leaf&lt;string&gt;</c> that writes lines to the file at
    /// <c>path</c> using the supplied <c>options</c>.
    /// </summary>
    let sink (path: string) (options: FileLeafOptions) : Leaf<string> =
        sinkFrom (fun () ->
            new StreamWriter(path, options.Append, options.Encoding) :> TextWriter)

    /// <summary>
    /// Creates a <c>Leaf&lt;string&gt;</c> that overwrites the file at <c>path</c>
    /// using UTF-8 without BOM. Equivalent to <c>File.sink path FileLeafOptions.Default</c>.
    /// </summary>
    let sinkDefault (path: string) : Leaf<string> =
        sink path FileLeafOptions.Default
