using System.Runtime.InteropServices;

namespace OxiDb.Client.Embedded;

internal static partial class NativeInterop
{
    private const string LibName = "oxidb_embedded_ffi";

    [LibraryImport(LibName, EntryPoint = "oxidb_open", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Open(string path);

    [LibraryImport(LibName, EntryPoint = "oxidb_open_encrypted", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint OpenEncrypted(string path, string keyPath);

    [LibraryImport(LibName, EntryPoint = "oxidb_close")]
    internal static partial void Close(nint handle);

    [LibraryImport(LibName, EntryPoint = "oxidb_execute", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Execute(nint handle, string cmdJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_free_string")]
    internal static partial void FreeString(nint ptr);
}
