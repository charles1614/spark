//
// Created by xialb on 7/11/21.
//

#include <pmix.h>
#include <stdio.h>


int main() {

    int rc;
    pid_t pid;
    pmix_proc_t myproc;
    pmix_info_t *info;
    size_t ninfo;
    pid = getpid();
    fprintf(stderr, "Client %lu: Running\n", (unsigned long) pid);

    ninfo = 2;

    int n = 0;
    PMIX_INFO_CREATE(info, ninfo);
    PMIX_INFO_LOAD(&info[n], PMIX_SERVER_URI, "PMIX_SERVER_URI2:;prterun-lenovo-95714@0.0;tcp4://192.168.32.197:44423", PMIX_STRING);
    ++n;


    /* server env */

//    PMIx_Allocation_request()

    setenv("PMIX_NAMESPACE", "prterun-lenovo-95714@1", 1);
    setenv("PMIX_RANK", "2", 1);
    setenv("PMIX_SERVER_URI2", "prterun-lenovo-95714@0.0;tcp4://192.168.32.197:44423", 1);
    setenv("PMIX_DSTORE_ESH_BASE_PATH", "/tmp/prte.lenovo.1000/dvm.95714/pmix_dstor_ds12_95714", 1);
//    setenv("PMIX_SECURITY_MODE", "native", 1);
//    setenv("PMIX_BFROP_BUFFER_TYPE", "PMIX_BFROP_BUFFER_FULLY_DESC", 1);
//    setenv("PMIX_GDS_MODULE", "ds21,ds12,hash", 1);
//    setenv("PMIX_SERVER_TMPDIR", "/tmp/prte.lenovo.1000/dvm.90556", 1);
//    setenv("PMIX_SYSTEM_TMPDIR", "/tmp", 1);
//    setenv("PMIX_DSTORE_21_BASE_PATH", "/tmp/prte.lenovo.1000/dvm.90556/pmix_dstor_ds21_90556", 1);
//    setenv("PMIX_DSTORE_ESH_BASE_PATH", "/tmp/prte.lenovo.1000/dvm.90556/pmix_dstor_ds12_90556", 1);
//    setenv("PMIX_HOSTNAME", "lenovo", 1);
//    setenv("PMIX_VERSION", "4.1.0rc1", 1);

    /* init us, return job-related info provided by RM */
    if (PMIX_SUCCESS != (rc = PMIx_Init(&myproc, info, ninfo))) {
        fprintf(stderr, "Client ns %s rank %d: PMIx_Init failed: %s\n", myproc.nspace, myproc.rank,
                PMIx_Error_string(rc));
        exit(0);
    } else {
        fprintf(stderr, "Client ns %s rank %d pid %lu: Running\n", myproc.nspace, myproc.rank,
                (unsigned long) pid);
    }

    /* set our hostname */
    char *ev1 = NULL;
    int ret;
    uint16_t u16, *u16ptr;
    pmix_info_t _info;
    pmix_value_t *_kv = NULL;
    PMIx_Get(&myproc, "PMIX_NSPACE",&_info, 1, &(_kv));




    done:
    /* finalize us */
//    sleep(10);
    fprintf(stderr, "Client ns %s rank %d: Finalizing\n", myproc.nspace, myproc.rank);
    PMIx_Finalize(NULL, 0);
    if (PMIX_SUCCESS != (rc = PMIx_Finalize(NULL, 0))) {
        fprintf(stderr, "Client ns %s rank %d:PMIx_Finalize failed: %d\n", myproc.nspace,
                myproc.rank, rc);
    } else {
        fprintf(stderr, "Client ns %s rank %d:PMIx_Finalize successfully completed\n",
                myproc.nspace, myproc.rank);
    }
    fflush(stderr);

    return rc;
}

