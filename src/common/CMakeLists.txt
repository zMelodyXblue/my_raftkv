# common is header-only; use an INTERFACE library so dependents get
# the include path automatically via target_link_libraries.
add_library(common INTERFACE)
target_include_directories(common INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/..   # allows #include "common/types.h"
)
