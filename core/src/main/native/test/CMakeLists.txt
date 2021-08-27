project(tool)
cmake_minimum_required(VERSION 3.17)
set(CMAKE_CXX_STANDARD 14)

include_directories(/docker/git/ompi/3rd-party/prrte/src/mca/ /docker/git/ompi/3rd-party/prrte/src/include /docker/git/ompi/3rd-party/prrte/)
#include_directories(/docker/git/ompi/3rd-party/openpmix/ /docker/git/ompi/3rd-party/openpmix/src/include/)

link_directories("/opt/deps/openpmix/lib" "/opt/deps/prrte/lib" "/usr/lib")
link_libraries("/opt/deps/openpmix/lib/libpmix.so")
link_libraries("/opt/deps/prrte/lib/libprrte.so")
link_libraries("/usr/lib/libevent_core-2.1.so.7")
link_libraries("/opt/ompi/lib/libmpi.so")

link_libraries("/opt/deps/openpmix/lib/libpmix.so")
link_libraries("/opt/deps/prrte/lib/libprrte.so")

add_executable(tool tool.c )
add_executable(hello hello.c)
add_executable(hello1 hello1.c)
add_executable(hello2 hello2.c)
add_executable(hello3 hello3.c)
add_executable(client client.c )