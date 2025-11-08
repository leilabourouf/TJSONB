#ifndef DETOAST_STUB_H
#define DETOAST_STUB_H

/*
 * Standalone build: disable detoasting logic.
 * In the real PostgreSQL backend, these macros expand to memory detoasting
 * and palloc-based copying. Here, they are no-ops.
 */

#ifndef PG_DETOAST_DATUM
#define PG_DETOAST_DATUM(x) (x)
#endif

#ifndef PG_DETOAST_DATUM_COPY
#define PG_DETOAST_DATUM_COPY(x) (x)
#endif

#ifndef PG_DETOAST_DATUM_PACKED
#define PG_DETOAST_DATUM_PACKED(x) (x)
#endif

#ifndef PG_DETOAST_DATUM_SLICE
#define PG_DETOAST_DATUM_SLICE(x, a, b) (x)
#endif

#endif /* DETOAST_STUB_H */
