#ifdef USE_REAL_POSTGRES
#error "Stub header included in real PostgreSQL build"
#endif
#ifndef USE_REAL_POSTGRES
#ifndef BACKEND_STUB_H
#define BACKEND_STUB_H

/*
 * Stand-alone PostgreSQL stubs for backend-only symbols.
 * These remove dependencies on the full backend runtime.
 */

#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdarg.h>   /* needed for va_list, va_start, va_end */

/* ---------------------------
 * ResourceOwner stubs
 * --------------------------- */
typedef void *ResourceOwner;

typedef struct ResourceOwnerDesc
{
    const char *name;
    int release_phase;
    int release_priority;
} ResourceOwnerDesc;

#define RESOURCE_RELEASE_AFTER_LOCKS 0
#define RELEASE_PRIO_FILES 0

#define ResourceOwnerRemember(owner, datum, desc) ((void)0)
#define ResourceOwnerForget(owner, datum, desc)   ((void)0)

/* ---------------------------
 * Memory / fsync flags
 * --------------------------- */
static const int enableFsync = 1;

/* ---------------------------
 * Logging & error stubs
 * --------------------------- */
#undef elog
#undef fprintf /* Avoid pg_fprintf interference */

static inline void elog_stub(int level, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    fprintf(stderr, "[elog %d] ", level);
    vfprintf(stderr, fmt, args);
    fprintf(stderr, "\n");
    va_end(args);
}

/* Safe macro alias */
#define elog(level, ...) elog_stub(level, __VA_ARGS__)
#define HAVE_ELOG_STUB 1

#define ereport(level, rest)  fprintf(stderr, "[ereport %d]\n", level)

/* ---------------------------
 * Misc backend hooks
 * --------------------------- */
#define before_shmem_exit(f, arg) ((void)0)
#define pgaio_closing_fd(fd)      ((void)0)
#define pgstat_report_tempfile(sz) ((void)0)

/* ---------------------------
 * Datum conversion fallback (only for builds without postgres.h)
 * --------------------------- */
#ifndef POSTGRES_H
/* Only define this if postgres.h wasn’t included */
static inline long PG_STUB_Int32GetDatum(long val) { return val; }
#endif


#endif /* BACKEND_STUB_H */
#endif /* !USE_REAL_POSTGRES */