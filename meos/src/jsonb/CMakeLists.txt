set(JSONB_SRCS
  jsonb_op.c
  jsonb_funcs.c
  jsonbset.c
  # jsonpath_exec.c
  tjsonb.c
  # tjsonb_jsonpath.c
  # tcbuffer_aggfuncs.c
  # tjsonb_boxops.c
  tjsonb_compops.c
)

add_library(jsonb OBJECT ${JSONB_SRCS})
