set(JSONB_SRCS
  jsonbset.c
  tjsonb.c
  # tcbuffer_aggfuncs.c
  # tjsonb_boxops.c
  tjsonb_compops.c
)

add_library(jsonb OBJECT ${JSONB_SRCS})
