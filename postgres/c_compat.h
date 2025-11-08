#ifndef C_COMPAT_H
#define C_COMPAT_H

/* Provide alignment constants missing in standalone builds */
#ifndef MAXIMUM_ALIGNOF
#define MAXIMUM_ALIGNOF 8
#endif

#ifndef TYPEALIGN
#define TYPEALIGN(ALIGNVAL, LEN)  \
    (((uintptr_t)(LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t)((ALIGNVAL) - 1)))
#endif

#ifndef MAXALIGN
#define MAXALIGN(LEN) TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
#endif

#endif /* C_COMPAT_H */
