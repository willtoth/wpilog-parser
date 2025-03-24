cmake_minimum_required(VERSION 3.22)

#########################
# Global settings
#########################
set(CMAKE_CXX_STANDARD              20)
set(CMAKE_CXX_STANDARD_REQUIRED     ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING "" FORCE)
endif()

option(WITH_TESTS "Build unit tests" OFF)

project(wpilogpy)

# Common compile options and include directories
# set(COMMON_COMPILE_OPTIONS "-fno-omit-frame-pointer" "-march=native") # "-march=native" AVX2 --> "-march=haswell" "-maes" "-march=alderlake" "-std=c++20" 

#########################
# Fetch all dependencies
#########################
include(FetchContent)

add_subdirectory(cmake/allwpilib)

find_package(Arrow REQUIRED)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)

# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

FetchContent_Declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt
  GIT_TAG        e69e5f977d458f2650bb346dadf2ad30c5320281) # 10.2.1
FetchContent_MakeAvailable(fmt)

#########################
# wpilogpy
#########################
add_executable(wpilogpy src/main.cpp)
target_compile_options(wpilogpy PUBLIC ${COMMON_COMPILE_OPTIONS})
target_link_libraries(wpilogpy PRIVATE wpiutil fmt::fmt Arrow::arrow_shared)

#########################
# test
#########################
if(WITH_TESTS)
    enable_testing()
    include(GoogleTest)

    file(GLOB_RECURSE test_source test/*_test.cpp)

    add_executable(wpilogpy_test ${test_source})
    target_link_libraries(wpilogpy_test GTest::gtest_main)

    gtest_discover_tests(wpilogpy_test)
endif()