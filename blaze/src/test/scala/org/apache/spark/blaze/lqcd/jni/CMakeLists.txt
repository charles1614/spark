project(lqcd_jni)

cmake_minimum_required(VERSION 2.8)

set(CMAKE_C_FLAGS "-std=c99 -lsimplelqcd -lm" )
set(CMAKE_INSTALL_PREFIX /home/xia/opt/lqcd)

add_library(read_file_c SHARED ReadFileC.c main.c)
add_executable(main main.c)

install(TARGETS read_file_c DESTINATION lib)