
add_subdirectory("mb")

set(UTILS_SRCS
  bool.c
  dynahash.c
  int.c
  int8.c
  date.c
  datetime.c
  float.c
  # format_type.c
  formatting.c
  numeric.c
  numutils.c
  pg_error.c
  pg_locale.c
  pg_locale_builtin.c
  pg_locale_libc.c
  pg_locale_icu.c
  timestamp.c
  varlena.c
  )

if(${POSTGRESQL_VERSION_NUMBER} GREATER_EQUAL 160000)
  list(APPEND UTILS_SRCS
    json.c
    jsonb.c
    jsonb_op.c
    jsonb_util.c
    jsonfuncs.c
)
endif()

# macOS-only include paths
if(APPLE)
  include_directories(
    ${CMAKE_SOURCE_DIR}/postgres
    ${CMAKE_SOURCE_DIR}/postgres/common
  )
endif()

add_library(utils OBJECT ${UTILS_SRCS})

# macOS-only: completely silence Clang for date.c 
if(APPLE)
  get_target_property(UTILS_SOURCES utils SOURCES)
  foreach(src ${UTILS_SOURCES})
    if(src MATCHES "date\\.c$")
      set_source_files_properties(${src} PROPERTIES COMPILE_FLAGS "-w -Wno-everything")
    endif()
  endforeach()

  # Also force the entire utils target to never treat warnings as errors
  target_compile_options(utils PRIVATE -Wno-everything -w)
endif()
