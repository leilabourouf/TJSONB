#ifndef PG_LOCALE_BUILTIN_COMPAT_H
#define PG_LOCALE_BUILTIN_COMPAT_H

#include <stdbool.h>
#include <wctype.h>

/*
 * Minimal replacement for PostgreSQL’s pg_u_isalnum().
 * This checks if a Unicode codepoint is alphanumeric.
 * The second argument (posix) is ignored for standalone builds.
 */
static inline bool
pg_u_isalnum(unsigned int codepoint, bool posix)
{
    (void)posix;
    if (codepoint < 128)
        return (('A' <= codepoint && codepoint <= 'Z') ||
                ('a' <= codepoint && codepoint <= 'z') ||
                ('0' <= codepoint && codepoint <= '9'));

    /* Use C library wide character classification */
    return iswalnum((wint_t) codepoint) != 0;
}

#endif /* PG_LOCALE_BUILTIN_COMPAT_H */
