# Find the loguru library (https://github.com/emilk/loguru)
#
# Sets the usual variables expected for find_package scripts:
#
# LOGURU_INCLUDE_DIR - header location
# LOGURU_LIBRARIES - libraries needed to use loguru
# LOGURU_FOUND - true if loguru was found.

find_path(LOGURU_INCLUDE_DIR loguru.hpp)
find_library(LOGURU_LIBRARIES NAMES loguru libloguru)

# Support the REQUIRED and QUIET arguments, and set LOGURU_FOUND if found.
include (FindPackageHandleStandardArgs)
find_package_handle_standard_args(loguru DEFAULT_MSG LOGURU_INCLUDE_DIR LOGURU_LIBRARIES)

mark_as_advanced(LOGURU_INCLUDE_DIR LOGURU_LIBRARIES)
