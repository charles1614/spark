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

    double t1, t2, t3;
    t1 = MPI_Wtime();
//    int ninfo = 2;
//    pmix_info_t *info;

//    int n = 0;
//    PMIX_INFO_CREATE(info, ninfo);
//    PMIX_INFO_LOAD(&info[n], PMIX_SERVER_URI, "PMIX_SERVER_URI2:;prterun-lenovo-97102@0.0;tcp4://192.168.32.197:44423",
//                   PMIX_STRING);
//    ++n;


    /* server env */

//    PMIx_Allocation_request()
    FILE *f = fopen("/home/xialb/pmixsrv.env", "r");
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

    setenv("PMIX_RANK", "0", 1);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    t2 = MPI_Wtime();


    int point = INT32_MAX;
    int my_point = point / size;


    if (size - 1 == rank) {
        my_point = point - my_point * rank;
    }

    srand(rank + 1);
    int cnt = 0;
    float r0, r1;
    for (int i = 0; i <= my_point; i++) {
        r0 = (float) rand() / INT32_MAX;
        r1 = (float) rand() / INT32_MAX;
        if (r0 * r0 + r1 * r1 <= 1) {
            ++cnt;
        }
    }

    int send = cnt;
    int recv = 0;


    MPI_Send(&send, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
    MPI_Allreduce(&send, &recv, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    t3 = MPI_Wtime();
    if (rank == 0) {
        printf("pi is %f\n", (double) recv / point * 4);
        printf("init time is %f\n", t2 - t1);
        printf("elapse time is %f\n", t3 - t1);
    }
    MPI_Finalize();

    return 0;
}