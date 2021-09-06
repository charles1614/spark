//
// Created by xialb on 7/12/21.
//
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 *
 * Sample MPI "hello world" application in C
 */

#include <stdio.h>
#include <pmix_common.h>
#include <fcntl.h>
#include "mpi.h"


int main(int argc, char *argv[]) {
    int rank, size;
    char version[MPI_MAX_LIBRARY_VERSION_STRING];

//    int ninfo = 2;
//    pmix_info_t *info;
//
//    int n = 0;
//    PMIX_INFO_CREATE(info, ninfo);
//    PMIX_INFO_LOAD(&info[n], PMIX_SERVER_URI, "PMIX_SERVER_URI2:;prterun-lenovo-97102@0.0;tcp4://192.168.32.197:44423",
//                   PMIX_STRING);
//    ++n;


    /* server env */

//    PMIx_Allocation_request()
    FILE *f = fopen("/tmp/pmixsrv.env", "r");
    char *line = NULL;
    size_t len;


    if (f == NULL) {
        perror("Unable to open file");
        exit(1);
    }
    while (-1 != getline(&line, &len, f)) {
        printf("%s", line);
        const char *delimiters = "=\n";
        char *k, *v;
        k = strtok(line, delimiters);
        v = strtok(NULL, delimiters);
        setenv(k, v, 1);
    }
    fclose(f);
    free(line);

    setenv("PMIX_RANK", "2", 1);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Get_library_version(version, &len);
    printf("Hello, world, I am %d of %d, (%s, %d)\n",
           rank, size, version, len);
    MPI_Finalize();

    exit(0);

    return 0;
}
