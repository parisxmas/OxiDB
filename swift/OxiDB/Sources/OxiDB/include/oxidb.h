#ifndef OXIDB_H
#define OXIDB_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque connection handle */
typedef void OxiDbConn;

/* Connect to an OxiDB server. Returns NULL on failure. */
OxiDbConn* oxidb_connect(const char* host, uint16_t port);

/* Disconnect and free the connection handle. */
void oxidb_disconnect(OxiDbConn* conn);

/* All functions below return a JSON string (caller must free with oxidb_free_string).
   Returns NULL on connection error. */

char* oxidb_ping(OxiDbConn* conn);

char* oxidb_insert(OxiDbConn* conn, const char* collection, const char* doc_json);
char* oxidb_insert_many(OxiDbConn* conn, const char* collection, const char* docs_json);
char* oxidb_find(OxiDbConn* conn, const char* collection, const char* query_json);
char* oxidb_find_one(OxiDbConn* conn, const char* collection, const char* query_json);
char* oxidb_update(OxiDbConn* conn, const char* collection, const char* query_json,
                   const char* update_json);
char* oxidb_update_one(OxiDbConn* conn, const char* collection, const char* query_json,
                       const char* update_json);
char* oxidb_delete(OxiDbConn* conn, const char* collection, const char* query_json);
char* oxidb_delete_one(OxiDbConn* conn, const char* collection, const char* query_json);
char* oxidb_count(OxiDbConn* conn, const char* collection);

char* oxidb_create_index(OxiDbConn* conn, const char* collection, const char* field);
char* oxidb_create_unique_index(OxiDbConn* conn, const char* collection, const char* field);
char* oxidb_create_composite_index(OxiDbConn* conn, const char* collection,
                                   const char* fields_json);
char* oxidb_create_text_index(OxiDbConn* conn, const char* collection,
                              const char* fields_json);
char* oxidb_list_indexes(OxiDbConn* conn, const char* collection);
char* oxidb_drop_index(OxiDbConn* conn, const char* collection, const char* index_name);

/* Collection-level text search */
char* oxidb_text_search(OxiDbConn* conn, const char* collection, const char* query,
                        int32_t limit);

char* oxidb_create_collection(OxiDbConn* conn, const char* collection);
char* oxidb_list_collections(OxiDbConn* conn);
char* oxidb_drop_collection(OxiDbConn* conn, const char* collection);
char* oxidb_compact(OxiDbConn* conn, const char* collection);

char* oxidb_aggregate(OxiDbConn* conn, const char* collection, const char* pipeline_json);

/* Blob storage */
char* oxidb_create_bucket(OxiDbConn* conn, const char* bucket);
char* oxidb_list_buckets(OxiDbConn* conn);
char* oxidb_delete_bucket(OxiDbConn* conn, const char* bucket);
char* oxidb_put_object(OxiDbConn* conn, const char* bucket, const char* key,
                       const char* data_b64, const char* content_type,
                       const char* metadata_json);
char* oxidb_get_object(OxiDbConn* conn, const char* bucket, const char* key);
char* oxidb_head_object(OxiDbConn* conn, const char* bucket, const char* key);
char* oxidb_delete_object(OxiDbConn* conn, const char* bucket, const char* key);
char* oxidb_list_objects(OxiDbConn* conn, const char* bucket, const char* prefix,
                         int32_t limit);

/* Full-text search */
char* oxidb_search(OxiDbConn* conn, const char* query, const char* bucket,
                   int32_t limit);

/* Transactions */
char* oxidb_begin_tx(OxiDbConn* conn);
char* oxidb_commit_tx(OxiDbConn* conn);
char* oxidb_rollback_tx(OxiDbConn* conn);

/* Free a string returned by any oxidb_* function. */
void oxidb_free_string(char* ptr);

#ifdef __cplusplus
}
#endif

#endif /* OXIDB_H */
