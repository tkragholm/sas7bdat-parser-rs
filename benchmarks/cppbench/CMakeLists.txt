cmake_minimum_required(VERSION 3.16)
project(SasCppBench VERSION 1.0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(LIB_CPP_DIR "${CMAKE_CURRENT_LIST_DIR}/../lib/cpp")
set(LIB_CPP_SRC_DIR "${LIB_CPP_DIR}/src")
set(LIB_CPP_INCLUDE_DIR "${LIB_CPP_DIR}/include")
set(GENERATED_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

file(MAKE_DIRECTORY "${GENERATED_INCLUDE_DIR}/cppsas7bdat")

configure_file(
  "${LIB_CPP_INCLUDE_DIR}/cppsas7bdat/version.hpp.in"
  "${GENERATED_INCLUDE_DIR}/cppsas7bdat/version.hpp"
  @ONLY
)

add_library(cppsas7bdat STATIC
  "${LIB_CPP_SRC_DIR}/column.cpp"
  "${LIB_CPP_SRC_DIR}/datasource_ifstream.cpp"
  "${LIB_CPP_SRC_DIR}/encodings.cpp"
  "${LIB_CPP_SRC_DIR}/exceptions.cpp"
  "${LIB_CPP_SRC_DIR}/reader.cpp"
  "${LIB_CPP_SRC_DIR}/stream.cpp"
  "${LIB_CPP_SRC_DIR}/types.cpp"
  "${LIB_CPP_SRC_DIR}/version.cpp"
)

target_include_directories(cppsas7bdat
  PUBLIC
    "${LIB_CPP_INCLUDE_DIR}"
    "${GENERATED_INCLUDE_DIR}"
  PRIVATE
    "${LIB_CPP_SRC_DIR}"
)

include(FetchContent)

if(NOT TARGET fmt::fmt)
  find_package(fmt CONFIG QUIET)
  if(NOT TARGET fmt::fmt)
    message(STATUS "fmt not found; fetching fmt 10.2.1")
    FetchContent_Declare(
      fmt
      GIT_REPOSITORY https://github.com/fmtlib/fmt.git
      GIT_TAG 10.2.1
    )
    FetchContent_MakeAvailable(fmt)
  endif()
endif()

if(TARGET fmt::fmt-header-only AND NOT TARGET fmt::fmt)
  add_library(fmt::fmt ALIAS fmt::fmt-header-only)
endif()

if(NOT TARGET spdlog::spdlog)
  find_package(spdlog CONFIG QUIET)
  if(NOT TARGET spdlog::spdlog)
    message(STATUS "spdlog not found; fetching spdlog v1.14.1")
    set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "" FORCE)
    FetchContent_Declare(
      spdlog
      GIT_REPOSITORY https://github.com/gabime/spdlog.git
      GIT_TAG v1.14.1
    )
    FetchContent_MakeAvailable(spdlog)
  endif()
endif()

if(TARGET spdlog::spdlog_header_only AND NOT TARGET spdlog::spdlog)
  add_library(spdlog::spdlog ALIAS spdlog::spdlog_header_only)
endif()

find_package(Boost REQUIRED COMPONENTS date_time)

target_link_libraries(cppsas7bdat
  PRIVATE
    fmt::fmt
    spdlog::spdlog
    Boost::date_time
)

target_compile_definitions(cppsas7bdat PRIVATE BOOST_DATE_TIME_NO_LIB)
target_include_directories(cppsas7bdat PUBLIC ${Boost_INCLUDE_DIRS})

add_executable(cpp_bench "${CMAKE_CURRENT_LIST_DIR}/src/main.cpp")
target_link_libraries(cpp_bench PRIVATE cppsas7bdat Boost::date_time)
