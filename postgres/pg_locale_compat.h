#ifndef PG_LOCALE_COMPAT_H
#define PG_LOCALE_COMPAT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <locale.h>
#include <time.h>

/* ----------------------------------------------------------------------
 * Stub definitions for PostgreSQL collation provider and memory API
 * ---------------------------------------------------------------------- */
#ifndef COLLPROVIDER_BUILTIN
#define COLLPROVIDER_BUILTIN "builtin"
#endif

#undef pfree
#define pfree(ptr) do { if ((void*)(uintptr_t)(ptr) != NULL && ((uintptr_t)(ptr) & 0xfff0000000000000ULL) != 0) free((void*)(uintptr_t)(ptr)); } while (0)

/* ----------------------------------------------------------------------
 * gettext / textdomain handling
 * ---------------------------------------------------------------------- */
#ifndef textdomain
static inline const char *textdomain(const char *domainname)
{
    (void)domainname;
    return "messages";
}
#endif

#if !defined(HAVE_PG_BIND_TEXTDOMAIN_CODESET)
#define HAVE_PG_BIND_TEXTDOMAIN_CODESET 1
#endif

/* ----------------------------------------------------------------------
 * ASCII check helper
 * ---------------------------------------------------------------------- */
#ifndef pg_is_ascii
static inline bool pg_is_ascii(const char *str)
{
    if (!str)
        return true;
    for (const unsigned char *p = (const unsigned char *)str; *p; p++)
    {
        if (*p > 127)
            return false;
    }
    return true;
}
#endif

/* ----------------------------------------------------------------------
 * strftime_l shim (macOS specific)
 * ---------------------------------------------------------------------- */
#ifndef HAVE_STRFTIME_L
#define HAVE_STRFTIME_L 1
static inline size_t strftime_l(char *buf, size_t maxsize,
                                const char *fmt, const struct tm *tm,
                                locale_t locale)
{
    /* macOS has strftime_l; on systems without it, fall back */
#ifdef __APPLE__
    return strftime_l(buf, maxsize, fmt, tm, locale);
#else
    (void)locale;
    return strftime(buf, maxsize, fmt, tm);
#endif
}
#endif

#endif /* PG_LOCALE_COMPAT_H */
