#ifndef LEVENSHTEIN_COMPAT_H
#define LEVENSHTEIN_COMPAT_H

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>

/* ----------------------------------------------------------------------
 * Basic PostgreSQL-type compatibility
 * ---------------------------------------------------------------------- */
#ifndef bool
typedef int bool;
#endif

#ifndef true
#define true 1
#endif
#ifndef false
#define false 0
#endif

#ifndef NULL
#define NULL ((void*)0)
#endif

#ifndef ERROR
#define ERROR 1
#endif

/* ----------------------------------------------------------------------
 * Memory management stubs
 * ---------------------------------------------------------------------- */
#ifndef palloc
#define palloc(sz) malloc(sz)
#endif

/* ----------------------------------------------------------------------
 * Character helpers
 * ---------------------------------------------------------------------- */
/* PostgreSQL multibyte-aware string length */
#ifndef pg_mblen
#define pg_mblen(str) 1  /* Assume 1-byte characters (UTF-8 safe default) */
#endif

#ifndef pg_mbstrlen_with_len
static inline int pg_mbstrlen_with_len(const char *str, int len)
{
    /* Count logical characters assuming UTF-8 single-byte fallback */
    return len;
}
#endif

#ifndef rest_of_char_same
static inline bool rest_of_char_same(const char *a, const char *b, int len)
{
    /* Compare remaining bytes of multi-byte chars */
    return (memcmp(a, b, len) == 0);
}
#endif

/* ----------------------------------------------------------------------
 * Logging stubs
 * ---------------------------------------------------------------------- */
#ifndef elog
#define elog(level, ...) fprintf(stderr, "[elog %d] ", level), fprintf(stderr, __VA_ARGS__), fprintf(stderr, "\n")
#endif

/* ----------------------------------------------------------------------
 * Math / utility macros
 * ---------------------------------------------------------------------- */
#ifndef Min
#define Min(x, y) ((x) < (y) ? (x) : (y))
#endif
#ifndef Max
#define Max(x, y) ((x) > (y) ? (x) : (y))
#endif

#endif /* LEVENSHTEIN_COMPAT_H */
