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

    [LibraryImport(LibName, EntryPoint = "oxidb_update_one", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint UpdateOne(nint conn, string collection, string queryJson, string updateJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_delete", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Delete(nint conn, string collection, string queryJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_delete_one", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint DeleteOne(nint conn, string collection, string queryJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_count", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Count(nint conn, string collection);

    [LibraryImport(LibName, EntryPoint = "oxidb_create_index", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint CreateIndex(nint conn, string collection, string field);

    [LibraryImport(LibName, EntryPoint = "oxidb_create_composite_index", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint CreateCompositeIndex(nint conn, string collection, string fieldsJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_create_unique_index", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint CreateUniqueIndex(nint conn, string collection, string field);

    [LibraryImport(LibName, EntryPoint = "oxidb_create_text_index", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint CreateTextIndex(nint conn, string collection, string fieldsJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_list_indexes", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint ListIndexes(nint conn, string collection);

    [LibraryImport(LibName, EntryPoint = "oxidb_drop_index", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint DropIndex(nint conn, string collection, string indexName);

    [LibraryImport(LibName, EntryPoint = "oxidb_text_search", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint TextSearch(nint conn, string collection, string query, int limit);

    [LibraryImport(LibName, EntryPoint = "oxidb_list_collections")]
    internal static partial nint ListCollections(nint conn);

    [LibraryImport(LibName, EntryPoint = "oxidb_create_collection", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint CreateCollection(nint conn, string collection);

    [LibraryImport(LibName, EntryPoint = "oxidb_drop_collection", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint DropCollection(nint conn, string collection);

    [LibraryImport(LibName, EntryPoint = "oxidb_aggregate", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Aggregate(nint conn, string collection, string pipelineJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_compact", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Compact(nint conn, string collection);

    // Blob storage + FTS

    [LibraryImport(LibName, EntryPoint = "oxidb_create_bucket", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint CreateBucket(nint conn, string bucket);

    [LibraryImport(LibName, EntryPoint = "oxidb_list_buckets")]
    internal static partial nint ListBuckets(nint conn);

    [LibraryImport(LibName, EntryPoint = "oxidb_delete_bucket", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint DeleteBucket(nint conn, string bucket);

    [LibraryImport(LibName, EntryPoint = "oxidb_put_object", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint PutObject(nint conn, string bucket, string key, string dataB64, string? contentType, string? metadataJson);

    [LibraryImport(LibName, EntryPoint = "oxidb_get_object", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint GetObject(nint conn, string bucket, string key);

    [LibraryImport(LibName, EntryPoint = "oxidb_head_object", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint HeadObject(nint conn, string bucket, string key);

    [LibraryImport(LibName, EntryPoint = "oxidb_delete_object", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint DeleteObject(nint conn, string bucket, string key);

    [LibraryImport(LibName, EntryPoint = "oxidb_list_objects", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint ListObjects(nint conn, string bucket, string? prefix, int limit);

    [LibraryImport(LibName, EntryPoint = "oxidb_search", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Search(nint conn, string query, string? bucket, int limit);

    // Transactions

    [LibraryImport(LibName, EntryPoint = "oxidb_begin_tx")]
    internal static partial nint BeginTx(nint conn);

    [LibraryImport(LibName, EntryPoint = "oxidb_commit_tx")]
    internal static partial nint CommitTx(nint conn);

    [LibraryImport(LibName, EntryPoint = "oxidb_rollback_tx")]
    internal static partial nint RollbackTx(nint conn);

    [LibraryImport(LibName, EntryPoint = "oxidb_free_string")]
    internal static partial void FreeString(nint ptr);
}
