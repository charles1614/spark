//
// Created by xialb on 7/11/21.
//

#ifndef TOOL_TOOL_H
#define TOOL_TOOL_H

#include "pmix_object.h"
#include "src/threads/threads.h"


typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    volatile bool active;
    pmix_status_t status;
} mylock_t;

/* define a structure for collecting returned
 * info from a query */
typedef struct {
    mylock_t lock;
    pmix_info_t *info;
    size_t ninfo;
} myquery_data_t;

typedef struct {
    pmix_object_t super;
    volatile bool active;
    pmix_proc_t caller;
    pmix_info_t *info;
    size_t ninfo;
    pmix_op_cbfunc_t cbfunc;
    pmix_spawn_cbfunc_t spcbfunc;
    void *cbdata;
} myxfer_t;

static void xfcon(myxfer_t *p) {
    p->info = NULL;
    p->ninfo = 0;
    p->active = true;
    p->cbfunc = NULL;
    p->spcbfunc = NULL;
    p->cbdata = NULL;
}

static void xfdes(myxfer_t *p) {
    if (NULL != p->info) {
        PMIX_INFO_FREE(p->info, p->ninfo);
    }
}

PMIX_CLASS_INSTANCE(myxfer_t, pmix_object_t, xfcon, xfdes);

#define PMIX_WAIT_FOR_COMPLETION(a) \
    do {                            \
        while ((a)) {               \
            usleep(10);             \
        }                           \
        PMIX_ACQUIRE_OBJECT((a));   \
    } while (0)


char *check_uri();

static void querycbfunc(pmix_status_t status, pmix_info_t *info, size_t ninfo, void *cbdata,
                        pmix_release_cbfunc_t release_fn, void *release_cbdata);

static void set_namespace(int nprocs, char *ranks, char *nspace, pmix_op_cbfunc_t cbfunc,
                          myxfer_t *x);

#endif //TOOL_TOOL_H
