project(blaze)

cmake_minimum_required(VERSION 3.17)

#aux_source_directory(. PRTE_SRC)
set(PRTE_SRC org_apache_spark_blaze_MPILauncher.c prte.c)
set(PRRTE /home/xialb/IdeaProjects/spark/blaze/src/main/native/prrte)
set(PMIX /opt/deps/pmix)

include_directories(${PRRTE} ${PRRTE}/src/include ${PMIX}/include)

link_directories("/opt/deps/pmix/lib" "/opt/deps/prrte/lib")
link_libraries("/opt/deps/pmix/lib/libpmix.so")

add_library(blaze SHARED ${PRTE_SRC})
target_link_libraries(blaze pmix prrte)

