#ifndef JSONB_COMPAT_H
#define JSONB_COMPAT_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

/* ----------------------------------------------------------------------
 * Stub for GetDatabaseEncoding()
 * PostgreSQL uses this to get the current database encoding ID.
 * For standalone builds, assume UTF8 (constant 6 in src/backend/utils/mb/encnames.c)
 * ---------------------------------------------------------------------- */
#ifndef GetDatabaseEncoding
#define GetDatabaseEncoding() 6
#endif

/* ----------------------------------------------------------------------
 * numeric_to_float4() stub
 * The PostgreSQL backend defines this in numeric.c; we mirror float8 logic.
 * ---------------------------------------------------------------------- */
#ifndef numeric_to_float4
static inline float numeric_to_float4(Numeric num)
{
    if (!num)
        return 0.0f;
    return (float) numeric_to_float8(num);
}
#endif

#endif /* JSONB_COMPAT_H */
