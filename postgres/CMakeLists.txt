# ------------------------------------------------------------
# Standalone PostgreSQL Compatibility Library (pgtypes)
# ------------------------------------------------------------
cmake_minimum_required(VERSION 3.12)
project(pgtypes C)

# ------------------------------------------------------------
# Detect platform-specific type sizes
# ------------------------------------------------------------
include(CheckTypeSize)

check_type_size("int" SIZEOF_INT)
check_type_size("long" SIZEOF_LONG)
check_type_size("long long" SIZEOF_LONG_LONG)
check_type_size("short" SIZEOF_SHORT)
check_type_size("bool" SIZEOF_BOOL)
check_type_size("void *" SIZEOF_VOID_P)
check_type_size("void *" SIZEOF_POINTER)
check_type_size("size_t" SIZEOF_SIZE_T)

# ------------------------------------------------------------
# Generate pg_config.h automatically
# ------------------------------------------------------------
set(PG_CONFIG_H "${CMAKE_CURRENT_SOURCE_DIR}/pg_config.h")

file(WRITE ${PG_CONFIG_H} "/* Auto-generated pg_config.h for portable standalone PostgreSQL build */\n")
# Guard everything so the header is ignored when building with real PostgreSQL
file(APPEND ${PG_CONFIG_H} "#ifndef PG_CONFIG_H\n#define PG_CONFIG_H\n\n")
file(APPEND ${PG_CONFIG_H} "#define PG_VERSION_NUM 170000\n")
file(APPEND ${PG_CONFIG_H} "#define PG_MAJORVERSION_NUM 17\n")
file(APPEND ${PG_CONFIG_H} "#define USE_FLOAT8_BYVAL 1\n")
file(APPEND ${PG_CONFIG_H} "#define USE_INTEGER_DATETIMES 1\n")
file(APPEND ${PG_CONFIG_H} "#define ENABLE_NLS 0\n")
file(APPEND ${PG_CONFIG_H} "#define HAVE_INT64 1\n")
file(APPEND ${PG_CONFIG_H} "#define USE_STDBOOL 1\n")
file(APPEND ${PG_CONFIG_H} "#define STDC_HEADERS 1\n")
file(APPEND ${PG_CONFIG_H} "#define MEMSET_LOOP_LIMIT 1024\n\n")

file(APPEND ${PG_CONFIG_H} "/* Platform type sizes (auto-detected) */\n")
file(APPEND ${PG_CONFIG_H} "#define SIZEOF_INT ${SIZEOF_INT}\n")
file(APPEND ${PG_CONFIG_H} "#define SIZEOF_LONG ${SIZEOF_LONG}\n")
file(APPEND ${PG_CONFIG_H} "#define SIZEOF_LONG_LONG ${SIZEOF_LONG_LONG}\n")
file(APPEND ${PG_CONFIG_H} "#define SIZEOF_SHORT ${SIZEOF_SHORT}\n")
file(APPEND ${PG_CONFIG_H} "#define SIZEOF_BOOL ${SIZEOF_BOOL}\n")
file(APPEND ${PG_CONFIG_H} "#define SIZEOF_POINTER ${SIZEOF_POINTER}\n")
file(APPEND ${PG_CONFIG_H} "#define SIZEOF_VOID_P ${SIZEOF_VOID_P}\n\n")
file(APPEND ${PG_CONFIG_H} "#define SIZEOF_SIZE_T ${SIZEOF_SIZE_T}\n")

file(APPEND ${PG_CONFIG_H} "/* Common feature macros */\n")
file(APPEND ${PG_CONFIG_H} "#define HAVE_GETTIMEOFDAY 1\n")
file(APPEND ${PG_CONFIG_H} "#define HAVE_STRING_H 1\n")
file(APPEND ${PG_CONFIG_H} "#define HAVE_STDINT_H 1\n")
file(APPEND ${PG_CONFIG_H} "#define HAVE_STDBOOL_H 1\n")
file(APPEND ${PG_CONFIG_H} "#define HAVE_SYS_TYPES_H 1\n")
file(APPEND ${PG_CONFIG_H} "#define HAVE_SYS_STAT_H 1\n")
file(APPEND ${PG_CONFIG_H} "#define HAVE_MEMORY_H 1\n")
file(APPEND ${PG_CONFIG_H} "#define HAVE_UNISTD_H 1\n")
file(APPEND ${PG_CONFIG_H} "#define HAVE_STRCHRNUL 1\n")
file(APPEND ${PG_CONFIG_H} "#define HAVE__GET_CPUID 1\n")
file(APPEND ${PG_CONFIG_H} "#define HAVE_X86_64_POPCNTQ 1\n\n")
file(APPEND ${PG_CONFIG_H} "#endif /* PG_CONFIG_H */\n")

message(STATUS "Created platform-specific pg_config.h with detected sizes:")
message(STATUS "  int: ${SIZEOF_INT}, long: ${SIZEOF_LONG}, long long: ${SIZEOF_LONG_LONG}, pointer: ${SIZEOF_POINTER}")

# ------------------------------------------------------------
# Collect PostgreSQL sources
# ------------------------------------------------------------
file(GLOB_RECURSE POSTGRES_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/*.c"
)

# Exclude backend-only or conflicting code
list(FILTER POSTGRES_SRC EXCLUDE REGEX ".*/storage/fd\\.c$")

# ------------------------------------------------------------
# Define the static library target
# ------------------------------------------------------------
add_library(pgtypes STATIC ${POSTGRES_SRC})

# Use the system timezone database
target_compile_definitions(pgtypes PUBLIC SYSTEMTZDIR=\"/usr/share/zoneinfo\")

target_compile_definitions(pgtypes PUBLIC MEOS=1)

set_target_properties(pgtypes PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_include_directories(pgtypes PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# ------------------------------------------------------------
# Compiler configuration
# ------------------------------------------------------------
target_compile_features(pgtypes PUBLIC c_std_11)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(pgtypes PRIVATE -w)
elseif(MSVC)
    target_compile_options(pgtypes PRIVATE /W0)
endif()

message(STATUS "Standalone PostgreSQL static library configured successfully.")
