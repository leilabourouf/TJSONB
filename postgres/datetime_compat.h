#ifndef DATETIME_COMPAT_H
#define DATETIME_COMPAT_H

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include <stdio.h>
#include <stdbool.h>
#include <math.h>

/* ----------------------------------------------------------------------
 * PostgreSQL integer overflow helpers
 * ---------------------------------------------------------------------- */
#ifndef pg_add_s32_overflow
static inline bool pg_add_s32_overflow(int32_t a, int32_t b, int32_t *result)
{
#if defined(__has_builtin)
#  if __has_builtin(__builtin_add_overflow)
    return __builtin_add_overflow(a, b, result);
#  endif
#endif
    int64_t r = (int64_t)a + (int64_t)b;
    *result = (int32_t)r;
    return (r > INT32_MAX || r < INT32_MIN);
}
#endif

#ifndef pg_mul_s32_overflow
static inline bool pg_mul_s32_overflow(int32_t a, int32_t b, int32_t *result)
{
#if defined(__has_builtin)
#  if __has_builtin(__builtin_mul_overflow)
    return __builtin_mul_overflow(a, b, result);
#  endif
#endif
    int64_t r = (int64_t)a * (int64_t)b;
    *result = (int32_t)r;
    return (r > INT32_MAX || r < INT32_MIN);
}
#endif

/* ----------------------------------------------------------------------
 * strtoint() — PostgreSQL helper for integer parsing
 * ---------------------------------------------------------------------- */
#ifndef strtoint
static inline int strtoint(const char *str, char **endptr, int base)
{
    return (int)strtol(str, endptr, base);
}
#endif

/* ----------------------------------------------------------------------
 * Memory allocation and NULL safety
 * ---------------------------------------------------------------------- */
#ifndef NULL
#define NULL ((void*)0)
#endif

#ifndef palloc
#define palloc(sz) malloc(sz)
#endif

/* ----------------------------------------------------------------------
 * Alignment constants (for MAXALIGN / TYPEALIGN)
 * ---------------------------------------------------------------------- */
#ifndef MAXIMUM_ALIGNOF
#define MAXIMUM_ALIGNOF 8
#endif

#ifndef TYPEALIGN
#define TYPEALIGN(ALIGNVAL,LEN)  \
    (((uintptr_t)(LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t)((ALIGNVAL) - 1)))
#endif

#ifndef MAXALIGN
#define MAXALIGN(LEN) TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
#endif

/* ----------------------------------------------------------------------
 * Math helpers
 * ---------------------------------------------------------------------- */
#ifndef Min
#define Min(a,b) ((a) < (b) ? (a) : (b))
#endif
#ifndef Max
#define Max(a,b) ((a) > (b) ? (a) : (b))
#endif

#endif /* DATETIME_COMPAT_H */
