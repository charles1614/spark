cmake_minimum_required(VERSION 3.17)

project(prrte)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)

add_subdirectory(src/tools/prte)

include_directories(include src/include)