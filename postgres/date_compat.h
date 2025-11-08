#ifndef DATE_COMPAT_H
#define DATE_COMPAT_H

#include <stdint.h>
#include <stdio.h>

/*
 * This header fixes type incompatibilities when compiling utils/date.c
 * standalone from PostgreSQL. It must be included AFTER all PostgreSQL headers.
 */

/* ----------------------------------------------------------------------
 * Numeric <-> Datum conversion stubs
 * ---------------------------------------------------------------------- */
#ifdef NumericGetDatum
#undef NumericGetDatum
#endif
#define NumericGetDatum(X) (X)

#ifdef DatumGetNumeric
#undef DatumGetNumeric
#endif
#define DatumGetNumeric(X) (X)

/* Dummy pg_numeric_in implementation */
#ifndef pg_numeric_in
#define pg_numeric_in(_s, _t) ((Numeric)NULL)
#endif

/* ----------------------------------------------------------------------
 * Infinity / special constants
 * ---------------------------------------------------------------------- */
#ifndef PG_INT64_MAX
#define PG_INT64_MAX INT64_MAX
#endif

/* Keep DT_NOEND as an integer for Timestamp/TimestampTz returns */
#ifdef DT_NOEND
#undef DT_NOEND
#endif
#define DT_NOEND ((int64)INT64_MAX)

/* Define a separate pointer sentinel for TimeTzADT* returns */
#ifndef DT_NOEND_PTR
#define DT_NOEND_PTR ((TimeTzADT *)(uintptr_t)INT64_MAX)
#endif

/* Patch macros that might use DT_NOEND for pointer-based results */
#ifdef TIMESTAMP_NOEND
#undef TIMESTAMP_NOEND
#endif
#define TIMESTAMP_NOEND(j) do { (j) = DT_NOEND; } while (0)

/* ----------------------------------------------------------------------
 * Handle NULL and gettext safely
 * ---------------------------------------------------------------------- */
#ifdef NULL
#undef NULL
#endif
#define NULL 0

#ifndef _
#define _(x) (x)
#endif
#ifndef gettext
#define gettext(x) (x)
#endif
#ifndef ngettext
#define ngettext(x,y,n) ((n)==1?(x):(y))
#endif

#endif /* DATE_COMPAT_H */
