#ifndef INT_COMPAT_H
#define INT_COMPAT_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

/* ----------------------------------------------------------------------
 * int32_out — format int32 as string
 * ---------------------------------------------------------------------- */
#ifndef int32_out
static inline char *
int32_out(int32_t value)
{
    static char buf[32];
    snprintf(buf, sizeof(buf), "%d", value);
    return buf;
}
#endif

/* ----------------------------------------------------------------------
 * int64_out — format int64 as string
 * ---------------------------------------------------------------------- */
#ifndef int64_out
static inline char *
int64_out(int64_t value)
{
    static char buf[64];
    snprintf(buf, sizeof(buf), "%lld", (long long)value);
    return buf;
}
#endif

/* ----------------------------------------------------------------------
 * float8_to_int64 — convert double to int64 safely
 * ---------------------------------------------------------------------- */
#ifndef float8_to_int64
static inline int64_t
float8_to_int64(double val)
{
    if (isnan(val))
        return 0;
    if (val > (double)INT64_MAX)
        return INT64_MAX;
    if (val < (double)INT64_MIN)
        return INT64_MIN;
    return (int64_t)val;
}
#endif

/* ----------------------------------------------------------------------
 * mul_int64_int64 — 64-bit multiply with overflow tolerance
 * ---------------------------------------------------------------------- */
#ifndef mul_int64_int64
static inline int64_t
mul_int64_int64(int64_t a, int64_t b)
{
    long double res = (long double)a * (long double)b;
    if (res > (long double)INT64_MAX)
        return INT64_MAX;
    if (res < (long double)INT64_MIN)
        return INT64_MIN;
    return (int64_t)res;
}
#endif

#endif /* INT_COMPAT_H */
