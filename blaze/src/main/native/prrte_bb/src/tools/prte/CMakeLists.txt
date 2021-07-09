project(blaze)

cmake_minimum_required(VERSION 3.17)

#aux_source_directory(. PRTE_SRC)
set(PRTE_SRC org_apache_spark_blaze_MPILauncher.c prte.c prun.c)
set(PRRTE /home/xialb/IdeaProjects/spark/blaze/src/main/native/prrte)
set(PMIX /opt/deps/pmix)

include_directories(${PRRTE} ${PRRTE}/src/include ${PMIX}/include)

link_directories("/opt/deps/pmix/lib" "/opt/deps/prrte/lib" "/usr/lib")
link_libraries("/opt/deps/pmix/lib/libpmix.so")
link_libraries("/opt/deps/prrte/lib/libprrte.so")
link_libraries("/usr/lib/libevent_core-2.1.so.7")
link_libraries("/usr/lib/libevent_pthreads-2.1.so.7" "/usr/lib/libpthread.so.0")


add_library(blaze SHARED ${PRTE_SRC})
target_link_libraries(blaze pmix prrte event)

#add_executable(prun prun.c)

