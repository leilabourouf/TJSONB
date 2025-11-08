#ifndef JSONFUNCS_COMPAT_H
#define JSONFUNCS_COMPAT_H

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include "postgres_types.h"

/* ----------------------------------------------------------------------
 * Stub: GetDatabaseEncoding()
 * PostgreSQL returns the database encoding ID; use UTF8 = 6.
 * ---------------------------------------------------------------------- */
#ifndef GetDatabaseEncoding
#define GetDatabaseEncoding() 6
#endif

/* ----------------------------------------------------------------------
 * Stub: pg_mblen()
 * Returns the byte length of a multibyte character. Assume UTF-8 (1 byte each).
 * ---------------------------------------------------------------------- */
#ifndef pg_mblen
#define pg_mblen(ptr) 1
#endif

/* ----------------------------------------------------------------------
 * Fix for elog() misuse
 * The PostgreSQL elog() function has signature: elog(int level, const char *fmt, ...).
 * If a call passes only a string literal, we wrap it correctly.
 * ---------------------------------------------------------------------- */
#ifndef elog
#define elog(level, ...) elog_stub(level, __VA_ARGS__)
#endif

/* ----------------------------------------------------------------------
 * Stub: cstring_to_text_with_len()
 * Allocates a simple text (varlena-like) buffer with the given length.
 * ---------------------------------------------------------------------- */
#ifndef cstring_to_text_with_len
static inline text *cstring_to_text_with_len(const char *str, size_t len)
{
    text *t = (text *)malloc(sizeof(text) + len + 1);
    if (!t)
        return NULL;
    memcpy(((char *)t) + sizeof(text), str, len);
    ((char *)t)[sizeof(text) + len] = '\0';
    /* Not a real varlena, but enough for standalone JSONB logic */
    return t;
}
#endif

#endif /* JSONFUNCS_COMPAT_H */
