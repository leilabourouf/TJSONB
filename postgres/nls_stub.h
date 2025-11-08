#ifndef NLS_STUB_H
#define NLS_STUB_H

/* Disable gettext / internationalization for standalone builds */
#ifndef ENABLE_NLS
#define ENABLE_NLS 0
#endif

#if ENABLE_NLS
#include <libintl.h>
#else
/* Neutral fallbacks */
#define _(x) (x)
#define gettext(x) (x)
#define dgettext(d, x) (x)
#define ngettext(x, y, n) ((n) == 1 ? (x) : (y))
#endif

#endif /* NLS_STUB_H */
