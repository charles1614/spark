//
// Created by xialb on 8/10/21.
//

#ifndef BLAZE_PMIX_INTERNAL_H
#define BLAZE_PMIX_INTERNAL_H

#include "src/pmix/pmix-internal.h"

static bool verbose = false;

static void infocb(pmix_status_t status, pmix_info_t *info, size_t ninfo, void *cbdata,
                   pmix_release_cbfunc_t release_fn, void *release_cbdata) {
    prte_pmix_lock_t *lock = (prte_pmix_lock_t *) cbdata;
#if PMIX_VERSION_MAJOR == 3 && PMIX_VERSION_MINOR == 0 && PMIX_VERSION_RELEASE < 3
    /* The callback should likely not have been called
     * see the comment below */
    if (PMIX_ERR_COMM_FAILURE == status) {
        return;
    }
#endif
    PRTE_ACQUIRE_OBJECT(lock);

    if (verbose) {
        prte_output(0, "PRUN: INFOCB");
    }

    if (NULL != release_fn) {
        release_fn(release_cbdata);
    }
    PRTE_PMIX_WAKEUP_THREAD(lock);
}

typedef struct {
    prte_pmix_lock_t lock;
    pmix_info_t *info;
    size_t ninfo;
} mylock_t;

/* define a structure for collecting returned
 * info from a query */
typedef struct {
    mylock_t lock;
    pmix_status_t status;
    pmix_info_t *info;
    size_t ninfo;
    pmix_app_t *apps;
    size_t napps;
} myquery_data_t;

/* this is a callback function for the PMIx_Query
 * API. The query will callback with a status indicating
 * if the request could be fully satisfied, partially
 * satisfied, or completely failed. The info parameter
 * contains an array of the returned data, with the
 * info->key field being the key that was provided in
 * the query call. Thus, you can correlate the returned
 * data in the info->value field to the requested key.
 *
 * Once we have dealt with the returned data, we must
 * call the release_fn so that the PMIx library can
 * cleanup */
static void cbfunc(pmix_status_t status, pmix_info_t *info, size_t ninfo, void *cbdata,
                   pmix_release_cbfunc_t release_fn, void *release_cbdata)
                   {
    myquery_data_t *mq = (myquery_data_t *) cbdata;
    size_t n;
    pmix_nspace_t ns;

//    printf("Called %s as callback for PMIx_Query\n", __FUNCTION__);
    mq->status = status;
    /* save the returned info - the PMIx library "owns" it
     * and will release it and perform other cleanup actions
     * when release_fn is called */
    if (0 < ninfo) {
        PMIX_INFO_CREATE(mq->info, ninfo);
        mq->ninfo = ninfo;
        for (n = 0; n < ninfo; n++) {
//            printf("Key %s Type %s(%d) Value %s\n", info[n].key,
//                   PMIx_Data_type_string(info[n].value.type), info[n].value.type,
//                   info[n].value.data.string);
            PMIX_INFO_XFER(&mq->info[n], &info[n]);
        }
    }

    /* let the library release the data and cleanup from
     * the operation */
//    if (NULL != release_fn) {
//        release_fn(release_cbdata);
//    }

    /* release the block */
    // DEBUG_WAKEUP_THREAD(&mq->lock);
                   }

#endif //BLAZE_PMIX_INTERNAL_H
