project(blaze)

set(CMAKE_C_FLAGS "")
set(CMAKE_CXX_FLAGS "")

set(OMPI_SRC "$ENV{MPI_SRC}")

# ORTE: Open Run-Time Environment
# OPAL: Open Portable Access Layer
# MCA: Modular Component Architecture
# ODLS: 

include_directories("${PROJECT_SOURCE_DIR}/include")
include_directories("/home/xialb/git/ompi/3rd-party/prrte")
include_directories("/home/xialb/git/ompi/3rd-party/prrte/src")
include_directories("/home/xialb/git/ompi/3rd-party/prrte/src/include")
include_directories("/home/xialb/git/ompi")
include_directories("/home/xialb/git/ompi/opal/include")
include_directories("/opt/deps/openpmix/include")

# Spark-MPI plugin

set(mca_odls_blaze_objs
        "prte/mca/odls/blaze/odls_blaze_module.c"
        "prte/mca/odls/blaze/odls_blaze_component.c"
        )

set(mca_rmaps_blaze_objs
        "prte/mca/rmaps/blaze/rmaps_blaze.c"
        "prte/mca/rmaps/blaze/rmaps_blaze_component.c"
        "prte/mca/rmaps/blaze/rmaps_blaze_lex.c"
        )

set(mca_rmaps_rank_file_objs
        "prte/mca/rmaps/rankfile/rmaps_rankfile_component.c"
        "prte/mca/rmaps/rankfile/rmaps_rankfile.c"
        "prte/mca/rmaps/rankfile/rmaps_rankfile_lex.c"
        )

#set(CMAKE_SHARED_LIBRARY_PREFIX "")

link_libraries("/opt/deps/openpmix/lib/libpmix.so")
link_libraries("/opt/deps/prrte/lib/libprrte.so")

add_library(mca_odls_blaze SHARED ${mca_odls_blaze_objs})
add_library(mca_rmaps_blaze SHARED ${mca_rmaps_blaze_objs})
add_library(mca_rmaps_rankfile SHARED ${mca_rmaps_rank_file_objs})
target_link_libraries(mca_odls_blaze pmix prrte)
target_link_libraries(mca_rmaps_blaze pmix prrte)
target_link_libraries(mca_rmaps_rankfile pmix prrte)
#INSTALL(TARGETS mca_odls_blaze LIBRARY DESTINATION lib)


