using System.Runtime.InteropServices;

namespace OxiDb.Client;

internal static partial class NativeInterop
{
    private const string LibName = "oxidb_client_ffi";

    [LibraryImport(LibName, EntryPoint = "oxidb_connect", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Connect(string host, ushort port);

    [LibraryImport(LibName, EntryPoint = "oxidb_disconnect")]
    internal static partial void Disconnect(nint conn);

    [LibraryImport(LibName, EntryPoint = "oxidb_ping")]
    internal static partial nint Ping(nint conn);

    [LibraryImport(LibName, EntryPoint = "oxidb_insert", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Insert(nint conn, string collection, string docJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_insert_many", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint InsertMany(nint conn, string collection, string docsJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_find", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Find(nint conn, string collection, string queryJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_find_one", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint FindOne(nint conn, string collection, string queryJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_update", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Update(nint conn, string collection, string queryJson, string updateJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_delete", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Delete(nint conn, string collection, string queryJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_count", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Count(nint conn, string collection);

    [LibraryImport(LibName, EntryPoint = "oxidb_create_index", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint CreateIndex(nint conn, string collection, string field);

    [LibraryImport(LibName, EntryPoint = "oxidb_create_composite_index", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint CreateCompositeIndex(nint conn, string collection, string fieldsJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_list_collections")]
    internal static partial nint ListCollections(nint conn);

    [LibraryImport(LibName, EntryPoint = "oxidb_drop_collection", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint DropCollection(nint conn, string collection);

    [LibraryImport(LibName, EntryPoint = "oxidb_aggregate", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Aggregate(nint conn, string collection, string pipelineJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_free_string")]
    internal static partial void FreeString(nint ptr);
}
