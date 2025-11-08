#pragma once
#include <stdint.h>
#include <stdbool.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

/* --- Overflow-safe helpers --- */
#ifndef pg_add_s32_overflow
static inline bool
pg_add_s32_overflow(int32_t a, int32_t b, int32_t *result)
{
    int64_t r = (int64_t)a + (int64_t)b;
    *result = (int32_t)r;
    return (r > INT32_MAX || r < INT32_MIN);
}
#endif

#ifndef pg_mul_s32_overflow
static inline bool
pg_mul_s32_overflow(int32_t a, int32_t b, int32_t *result)
{
    int64_t r = (int64_t)a * (int64_t)b;
    *result = (int32_t)r;
    return (r > INT32_MAX || r < INT32_MIN);
}
#endif

/* --- Datum / Numeric compatibility --- */
#ifndef Datum
typedef uintptr_t Datum;
#endif

#ifndef PointerGetDatum
#define PointerGetDatum(X) ((Datum)(uintptr_t)(X))
#endif

#ifndef DatumGetPointer
#define DatumGetPointer(X) ((void *)(uintptr_t)(X))
#endif

#ifndef Numeric
typedef struct NumericData *Numeric;
#endif

#ifndef NumericGetDatum
#define NumericGetDatum(X) PointerGetDatum(X)
#endif

#ifndef DatumGetNumeric
#define DatumGetNumeric(X) ((Numeric) DatumGetPointer(X))
#endif

/*
 * Stub for numeric_in() — no sizeof(struct NumericData),
 * since it's an opaque forward declaration.
 * We just allocate a generic small block to represent a dummy Numeric.
 */
#ifndef numeric_in_c
static inline Numeric numeric_in_c(const char *str, int typmod)
{
    (void)typmod;
    size_t len = strlen(str);
    void *mem = malloc(len + 32);
    if (mem)
        memset(mem, 0, len + 32);
    return (Numeric)mem;
}
#endif

/* --- pg_numeric_in wrapper --- */
/* It must return a Datum where the caller expects a Datum. */
#ifndef pg_numeric_in
#define pg_numeric_in(str, typmod) NumericGetDatum(numeric_in_c(str, typmod))
#endif
