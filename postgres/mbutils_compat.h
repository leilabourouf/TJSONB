#ifndef MBUTILS_COMPAT_H
#define MBUTILS_COMPAT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

/* ----------------------------------------------------------------------
 * Memory context emulation
 * ---------------------------------------------------------------------- */
#ifndef CurrentMemoryContext
/* PostgreSQL uses this for memory safety; we just stub it as non-null */
#define CurrentMemoryContext ((void*)1)
#endif

/* ----------------------------------------------------------------------
 * Logging and severity levels
 * ---------------------------------------------------------------------- */
#ifndef LOG
#define LOG 0
#endif

#ifndef ERROR
#define ERROR 1
#endif

#ifndef elog
#define elog(level, ...) fprintf(stderr, "[elog %d] ", level), fprintf(stderr, __VA_ARGS__), fprintf(stderr, "\n")
#endif

/* ----------------------------------------------------------------------
 * Write to stderr stub
 * ---------------------------------------------------------------------- */
#ifndef write_stderr
#define write_stderr(msg) fprintf(stderr, "%s\n", (msg))
#endif

/* ----------------------------------------------------------------------
 * gettext domain binding
 * ---------------------------------------------------------------------- */
#ifndef bind_textdomain_codeset
static inline int bind_textdomain_codeset(const char *domainname, const char *codeset)
{
    /* Pretend to succeed: standalone builds don't use gettext domains */
    (void)domainname;
    (void)codeset;
    return 0;
}
#endif

#endif /* MBUTILS_COMPAT_H */
