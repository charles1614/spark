cmake_minimum_required(VERSION 3.21)
project(blaze)

include_directories(/$ENV{HOME}/git/ompi/3rd-party/prrte/src/mca/ /$ENV{HOME}/git/ompi/3rd-party/prrte/src/include /$ENV{HOME}/git/ompi/3rd-party/prrte/)
include_directories(/$ENV{HOME}/git/ompi/3rd-party/openpmix/src/class /$ENV{HOME}/git/ompi/3rd-party/prrte/src/tools/prun/)

#aux_source_directory(. PRTE_SRC)
set(PRRTE /opt/deps/prrte)
set(PMIX /opt/deps/openpmix)
set(PRRTE_SRC /home/xialb/git/ompi/3rd-party/prrte)

set(PRTE_SRC ${PRRTE_SRC}/src/tools/prun/prun.c ${PRRTE_SRC}/src/util/prte_environ.c)
# set(SRC org_apache_spark_blaze_deploy_mpi_NativeUtils.c org_apache_spark_blaze_deploy_mpi_NativeUtils.cc include/pmix_internal.h)
set(SRC org_apache_spark_blaze_deploy_mpi_NativeUtils.cc include/pmix_internal.h)
set(CORE_SRC org_apache_spark_mpi_NativeUtil.h org_apache_spark_mpi_NativeUtil.c)
include_directories(${PRRTE} ${PRRTE}/include ${PRRTE_SRC} ${PRRTE_SRC}/src/include)
include_directories(${PMIX}/include)
include_directories(${PROJECT_SOURCE_DIR}/include)

if(NOT DEFINED ENV{JAVA_HOME})
    message(FATAL_ERROR "not defined JAVA_HOME")
endif()

include_directories($ENV{JAVA_HOME}/include $ENV{JAVA_HOME}/include/linux)

# link_directories("/opt/deps/openpmix/lib" "/opt/deps/prrte/lib" "/usr/lib")
# link_libraries("/opt/deps/openpmix/lib/libpmix.so")
# link_libraries("/opt/deps/prrte/lib/libprrte.so")
# link_libraries("/usr/lib64/libevent_core.so")

#link_libraries("/usr/lib/libevent_pthreads-2.1.so.7")

#add_library(blaze SHARED ${PRTE_SRC} ${SRC})
add_library(blaze SHARED ${PRTE_SRC} ${SRC} ${CORE_SRC} )
target_link_libraries(blaze prrte)
