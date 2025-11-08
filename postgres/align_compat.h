#ifndef ALIGN_COMPAT_H
#define ALIGN_COMPAT_H

#include <stddef.h>

/*
 * Default alignment assumptions for standalone builds.
 * PostgreSQL defines these in pg_config_manual.h or pg_config.h.
 * We assume standard platform alignments.
 */
#ifndef ALIGNOF_SHORT
#define ALIGNOF_SHORT 2
#endif

#ifndef ALIGNOF_INT
#define ALIGNOF_INT 4
#endif

#ifndef ALIGNOF_LONG
#define ALIGNOF_LONG 8
#endif

#ifndef ALIGNOF_DOUBLE
#define ALIGNOF_DOUBLE 8
#endif

#ifndef MAXIMUM_ALIGNOF
#define MAXIMUM_ALIGNOF 8
#endif

#endif /* ALIGN_COMPAT_H */
