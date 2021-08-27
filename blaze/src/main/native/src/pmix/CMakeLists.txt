project(blaze)

set(CMAKE_C_FLAGS "")
set(CMAKE_CXX_FLAGS "")

set(OMPI_SRC "$ENV{MPI_SRC}")

# ORTE: Open Run-Time Environment
# OPAL: Open Portable Access Layer
# MCA: Modular Component Architecture
# ODLS: 

include_directories("${PROJECT_SOURCE_DIR}/include")
include_directories("/docker/git/ompi/3rd-party/prrte")
include_directories("/docker/git/ompi/3rd-party/prrte/src")
include_directories("/docker/git/ompi/3rd-party/prrte/src/include")
include_directories("/docker/git/ompi")
include_directories("/docker/git/ompi/opal/include")

# Spark-MPI plugin

set(mca_odls_blaze_objs
        "prte/mca/odls/blaze/odls_blaze_module.c"
        "prte/mca/odls/blaze/odls_blaze_component.c"
        )

#set(CMAKE_SHARED_LIBRARY_PREFIX "")

link_libraries("/opt/deps/openpmix/lib/libpmix.so")
link_libraries("/opt/deps/prrte/lib/libprrte.so")

add_library(mca_odls_blaze SHARED ${mca_odls_blaze_objs})
target_link_libraries(mca_odls_blaze pmix prrte)
#INSTALL(TARGETS mca_odls_blaze LIBRARY DESTINATION lib)


