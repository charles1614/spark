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
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include "state/state.h"
#include <pmix.h>
#include "plm/plm_types.h"
#include <pmix_common.h>
#include "mpi.h"
//#include "src/include/pmix_globals.h"



int main(int argc, char *argv[]) {
    int rank, size;
    char version[MPI_MAX_LIBRARY_VERSION_STRING];

    int ninfo = 2;
    pmix_info_t *info;

    int n = 0;
    PMIX_INFO_CREATE(info, ninfo);
    PMIX_INFO_LOAD(&info[n], PMIX_SERVER_URI, "PMIX_SERVER_URI2:;prterun-lenovo-97102@0.0;tcp4://192.168.32.197:44423",
                   PMIX_STRING);
    ++n;


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

//    setenv("PMIX_NAMESPACE", "prterun-lenovo-97102@1", 1);
    setenv("PMIX_RANK", "1", 1);
//    setenv("PMIX_SERVER_URI2", "prterun-lenovo-97102@0.0;tcp4://192.168.32.197:33307", 1);
//    setenv("PMIX_DSTORE_ESH_BASE_PATH", "/tmp/prte.lenovo.1000/dvm.97102/pmix_dstor_ds12_97102", 1);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Get_library_version(version, &len);
    printf("Hello, world, I am %d of %d, (%s, %d)\n",
           rank, size, version, len);

//    pmix_proc_t pp;
//    struct pmix_info pinfo;
//    pmix_status_t ret;
//    bool flag = true;
//    PMIX_INFO_LOAD(&pinfo, PMIX_JOB_CTRL_TERMINATE, &flag, PMIX_BOOL);
//    ret = PMIx_Job_control(NULL, 0, &pinfo, 1, NULL, NULL);
//    if(PMIX_SUCCESS != ret){
//        fprintf(stderr, "JOB ctrl error");
//    }

//    PRTE_ACTIVATE_PROC_STATE(&prte_process_info.myproc, PRTE_PROC_STATE_TERMINATED);
    MPI_Finalize();
    return 0;
}
