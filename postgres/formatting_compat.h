#ifndef FORMATTING_COMPAT_H
#define FORMATTING_COMPAT_H

#include <stdint.h>
#include <stdbool.h>
#include <ctype.h>
#include <limits.h>
#include <math.h>

/* ----------------------------------------------------------------------
 * Alignment macros — PostgreSQL normally defines these in c.h
 * ---------------------------------------------------------------------- */
#ifndef MAXIMUM_ALIGNOF
/* Use the system’s native alignment; 8 works safely on 64-bit systems */
#define MAXIMUM_ALIGNOF 8
#endif

#ifndef TYPEALIGN
#define TYPEALIGN(ALIGNVAL, LEN)  \
    (((uintptr_t)(LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t)((ALIGNVAL) - 1)))
#endif

#ifndef MAXALIGN
#define MAXALIGN(LEN) TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
#endif

/* ----------------------------------------------------------------------
 * scanner_isspace replacement
 * PostgreSQL uses its own locale-safe version; we can use plain isspace.
 * ---------------------------------------------------------------------- */
#ifndef scanner_isspace
static inline bool
scanner_isspace(char c)
{
    return isspace((unsigned char)c) != 0;
}
#endif

/* ----------------------------------------------------------------------
 * Safe integer overflow helpers (if not already defined)
 * ---------------------------------------------------------------------- */
#ifndef pg_add_s32_overflow
static inline bool
pg_add_s32_overflow(int32_t a, int32_t b, int32_t *result)
{
    int64_t tmp = (int64_t)a + (int64_t)b;
    if (tmp > INT32_MAX || tmp < INT32_MIN)
        return true;
    if (result)
        *result = (int32_t)tmp;
    return false;
}
#endif

#ifndef pg_mul_s32_overflow
static inline bool
pg_mul_s32_overflow(int32_t a, int32_t b, int32_t *result)
{
    int64_t tmp = (int64_t)a * (int64_t)b;
    if (tmp > INT32_MAX || tmp < INT32_MIN)
        return true;
    if (result)
        *result = (int32_t)tmp;
    return false;
}
#endif

#endif /* FORMATTING_COMPAT_H */
