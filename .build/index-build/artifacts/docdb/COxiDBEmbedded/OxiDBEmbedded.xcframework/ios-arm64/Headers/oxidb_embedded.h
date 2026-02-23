#ifndef OXIDB_EMBEDDED_H
#define OXIDB_EMBEDDED_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque database handle */
typedef void OxiDbHandle;

/* Open a database at the given directory. Returns NULL on failure. */
OxiDbHandle* oxidb_open(const char* path);

/* Open a database with AES-GCM encryption.
   key_path points to a file containing a 32-byte key.
   Returns NULL on failure. */
OxiDbHandle* oxidb_open_encrypted(const char* path, const char* key_path);

/* Close the database and free the handle. Safe to call with NULL. */
void oxidb_close(OxiDbHandle* handle);

/* Execute a JSON command against the database.
   cmd_json is a JSON string using the same protocol as the OxiDB TCP server.
   Returns a JSON response string (caller must free with oxidb_free_string).
   Returns NULL only on internal error. */
char* oxidb_execute(OxiDbHandle* handle, const char* cmd_json);

/* Free a string returned by oxidb_execute. Safe to call with NULL. */
void oxidb_free_string(char* ptr);

#ifdef __cplusplus
}
#endif

#endif /* OXIDB_EMBEDDED_H */
