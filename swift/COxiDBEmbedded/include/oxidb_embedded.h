#ifndef OXIDB_EMBEDDED_H
#define OXIDB_EMBEDDED_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void OxiDbHandle;

OxiDbHandle* oxidb_open(const char* path);
OxiDbHandle* oxidb_open_encrypted(const char* path, const char* key_path);
void oxidb_close(OxiDbHandle* handle);
char* oxidb_execute(OxiDbHandle* handle, const char* cmd_json);
void oxidb_free_string(char* ptr);

#ifdef __cplusplus
}
#endif

#endif /* OXIDB_EMBEDDED_H */
