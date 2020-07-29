cmake_minimum_required(VERSION 3.1.0 FATAL_ERROR)

project(batsched CXX)

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

# Options
option(enable_warnings "Enable compilation warnings" ON)
option(treat_warnings_as_errors "Treat compilation warnings as compilation errors" OFF)
option(do_coverage "Compile in coverage mode" OFF)

# Build type
# Set a default build type if none was specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'Debug' as none was specified.")
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
    "MinSizeRel" "RelWithDebInfo")
endif()

# Let's enable C++11
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Coverage
if (do_coverage)
    set(coverage_cxxflags "-g -O0 -fprofile-arcs -ftest-coverage")
    set(coverage_ldflags "-lgcov --coverage")

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${coverage_cxxflags}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${coverage_ldflags}")
endif()

# Dependencies
## Boost
find_package(Boost 1.48 REQUIRED COMPONENTS regex locale)
include_directories(${Boost_INCLUDE_DIR})

## Intervalset
find_package(intervalset REQUIRED)
include_directories(${INTERVALSET_INCLUDE_DIR})

## loguru
find_package(loguru REQUIRED)
include_directories(${LOGURU_INCLUDE_DIR})

## pthread (for loguru)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

## GMP
find_package(GMP REQUIRED)
include_directories(${GMP_INCLUDE_DIR})

## RapidJSON
find_package(rapidjson REQUIRED)
include_directories(${RAPIDJSON_INCLUDE_DIRS})

## Redox dependency
find_package(redox REQUIRED)
include_directories(${REDOX_INCLUDE_DIR})

## Redox sub dependencies
find_package(hiredis REQUIRED)
include_directories(${HIREDIS_INCLUDE_DIRS})

find_package(libev REQUIRED)
include_directories(${LIBEV_INCLUDE_DIRS})

# ZeroMQ dependency
find_package(ZMQ REQUIRED)
include_directories(${ZMQ_INCLUDE_DIRS})

file(GLOB batsched_SRC
    "src/*.hpp"
    "src/*.cpp"
    "src/algo/*.hpp"
    "src/algo/*.cpp")

add_executable(batsched ${batsched_SRC})

target_link_libraries(batsched ${GMP_LIBRARIES}
                      ${Boost_REGEX_LIBRARY_DEBUG}
                      ${Boost_LOCALE_LIBRARY_DEBUG}
                      ${INTERVALSET_LIBRARY}
                      ${LOGURU_LIBRARIES}
                      Threads::Threads
                      ${CMAKE_DL_LIBS}
                      ${REDOX_LIBRARY}
                      ${LIBEV_LIBRARY}
                      ${HIREDIS_LIBRARY}
                      ${ZMQ_LIBRARIES})

target_link_libraries(batsched boost_program_options)

# Installation
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/batsched
        DESTINATION bin)

# Let's enable warnings if needed
if (enable_warnings)
    set(warning_flags " -Wall -Wextra")

    if (treat_warnings_as_errors)
        set(warning_flags "${warning_flags} -Werror")
    endif()

    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        set_property(TARGET batsched APPEND_STRING PROPERTY COMPILE_FLAGS ${warning_flags})
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        set_property(TARGET batsched APPEND_STRING PROPERTY COMPILE_FLAGS ${warning_flags})
    else()
        message(WARNING "Unknown compiler. Warnings should not be enabled correctly.")
        set_property(TARGET batsched APPEND_STRING PROPERTY COMPILE_FLAGS ${warning_flags})
    endif()
endif()

####################
# Batsched version #
####################
set(default_batsched_version "v1.4.0")
include(GetGitRevisionDescription)
git_describe(batsched_version)
message(STATUS "Batsched version from git: ${batsched_version}")

if(NOT((${batsched_version} STREQUAL "GIT-NOTFOUND") OR
       (${batsched_version} STREQUAL "HEAD-HASH-NOTFOUND")))
    add_definitions(-DBATSCHED_VERSION=${batsched_version})
else()
    message(WARNING "Cannot retrieve Batsched version from git. "
                    "Using default version ${default_batsched_version}")
    add_definitions(-DBATSCHED_VERSION=${default_batsched_version})
endif()
