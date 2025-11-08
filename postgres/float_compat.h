#ifndef FLOAT_COMPAT_H
#define FLOAT_COMPAT_H

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <limits.h>
#include <math.h>

/* ----------------------------------------------------------------------
 * lwprint_double()
 * ---------------------------------------------------------------------- */

#ifndef HAVE_LWPRINT_DOUBLE
static inline void
lwprint_double(double value, int maxdd, char *ascii)
{
    if (!ascii || maxdd <= 0)
        return;
    snprintf(ascii, maxdd, "%.17g", value);
}
#endif

/* ----------------------------------------------------------------------
 * Signed 32-bit overflow helpers
 * ----------------------------------------------------------------------
 * Some PostgreSQL branches (especially when built standalone)
 * only export the unsigned versions (pg_add_u32_overflow, etc.).
 * We add minimal signed variants here.
 */
#ifndef HAVE_PG_ADD_S32_OVERFLOW
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

#endif /* FLOAT_COMPAT_H */
