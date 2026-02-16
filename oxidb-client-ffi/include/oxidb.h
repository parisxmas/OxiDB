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
char* oxidb_delete(OxiDbConn* conn, const char* collection, const char* query_json);
char* oxidb_count(OxiDbConn* conn, const char* collection);

char* oxidb_create_index(OxiDbConn* conn, const char* collection, const char* field);
char* oxidb_create_composite_index(OxiDbConn* conn, const char* collection,
                                   const char* fields_json);

char* oxidb_list_collections(OxiDbConn* conn);
char* oxidb_drop_collection(OxiDbConn* conn, const char* collection);

/* Free a string returned by any oxidb_* function. */
void oxidb_free_string(char* ptr);

#ifdef __cplusplus
}
#endif

#endif /* OXIDB_H */
