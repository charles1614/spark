project(tool)
set(CMAKE_CXX_STANDARD 14)

include_directories($ENV{HOME}/git/ompi/3rd-party/prrte/src/mca/ $ENV{HOME}/git/ompi/3rd-party/prrte/src/include $ENV{HOME}/git/ompi/3rd-party/prrte/)
include_directories($ENV{HOME}/git/ompi/3rd-party/openpmix/src/class $ENV{HOME}/git/ompi/3rd-party/prrte/src/tools/prun/)

include_directories(${PRRTE} ${PRRTE}/include ${PRRTE_SRC} ${PRRTE_SRC}/src/include)
include_directories(${PMIX}/include)
include_directories($ENV{HOME}/git/spark/blaze/src/main/native/src/blaze/include)
#include_directories(/docker/git/ompi/3rd-party/openpmix/ /docker/git/ompi/3rd-party/openpmix/src/include/)

link_directories("/opt/deps/openpmix/lib" "/opt/deps/prrte/lib" "/usr/lib")
link_libraries("/opt/deps/openpmix/lib/libpmix.so")
link_libraries("/opt/deps/prrte/lib/libprrte.so")
link_libraries("/usr/lib64/libevent_core.so")
link_libraries("/opt/ompi/lib/libmpi.so")

link_libraries("/opt/deps/openpmix/lib/libpmix.so")
link_libraries("/opt/deps/prrte/lib/libprrte.so")

#add_executable(tool tool.c )
add_executable(hello0 hello0.c)
add_executable(hello1 hello1.c)
add_executable(hello2 hello2.c)
add_executable(hello3 hello3.c)
add_executable(prun_test prun_test.c prte_environ.c $ENV{HOME}/git/ompi/3rd-party/prrte/src/tools/prun/prun.c)
target_link_libraries(prun_test pmix prrte pthread)
#add_executable(client client.c )
