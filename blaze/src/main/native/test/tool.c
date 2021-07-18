//
// Created by xialb on 7/9/21.
//
#include <pmix.h>
#include <pmix_server.h>
#include <pmix_common.h>

#define __USE_GNU
#include "tool.h"
#include <stdio.h>
#include <pmix_tool.h>

char hostname[1024];
static char application_namespace[PMIX_MAX_NSLEN + 1];

int query_application_namespace(char *nspace)
{
    pmix_info_t *namespace_query_data;
    char *p;
    size_t namespace_query_size;
    pmix_status_t rc;
    pmix_query_t namespace_query;
    int wildcard_rank = PMIX_RANK_WILDCARD;
    int ninfo;
    int n;
    int len;

    printf("%s called to get application namespace\n", __FUNCTION__);
    PMIX_QUERY_CONSTRUCT(&namespace_query);
    PMIX_ARGV_APPEND(rc, namespace_query.keys, PMIX_QUERY_NAMESPACES);
    if (PMIX_SUCCESS != rc) {
        fprintf(stderr, "An error occurred creating namespace query.");
        PMIX_QUERY_DESTRUCT(&namespace_query);
        return -1;
    }
    PMIX_INFO_CREATE(namespace_query.qualifiers, 2);
    ninfo = 2;
    n = 0;
    PMIX_INFO_LOAD(&namespace_query.qualifiers[n], PMIX_NSPACE, nspace, PMIX_STRING);
    n++;
    PMIX_INFO_LOAD(&namespace_query.qualifiers[n], PMIX_RANK, &wildcard_rank, PMIX_INT32);
    namespace_query.nqual = ninfo;
    rc = PMIx_Query_info(&namespace_query, 1, &namespace_query_data, &namespace_query_size);
    PMIX_QUERY_DESTRUCT(&namespace_query);
    if (PMIX_SUCCESS != rc) {
        fprintf(stderr, "An error occurred querying application namespace: %s.\n",
                PMIx_Error_string(rc));
        return -1;
    }
    if ((1 != namespace_query_size) || (PMIX_STRING != namespace_query_data->value.type)) {
        fprintf(stderr, "The response to namespace query has wrong format.\n");
        return -1;
    }
    /* The query retruns a comma-delimited list of namespaces. If there are
     * multple namespaces in the list, then assume the first is the
     * application namespace and the second is the daemon namespace.
     * Copy only the application namespace and terminate the name with '\0' */
    p = strchr(namespace_query_data->value.data.string, ',');
    if (NULL == p) {
        len = strlen(namespace_query_data->value.data.string);
    } else {
        len = p - namespace_query_data->value.data.string;
    }
    strncpy(application_namespace, namespace_query_data->value.data.string, len);
    application_namespace[len] = '\0';
    printf("Application namespace is '%s'\n", application_namespace);
    return 0;
}

char *query_info(char *qkey, char *opt) {

    pmix_query_t *query;
    int nq = 1;
    int rc;
    char *nodename = "lenovo";
    myquery_data_t mydata;
    pmix_proc_t myproc;
    pmix_info_t *res;
    size_t s;
    bool nb = false;

    PMIX_INFO_CREATE(res, 1);

    PMIX_QUERY_CREATE(query, nq);
    PMIX_ARGV_APPEND(rc, query[0].keys, qkey);

    PMIX_QUERY_QUALIFIERS_CREATE(&query[0], 1);
//    PMIX_INFO_LOAD(&query[0].qualifiers[0], PMIX_HOSTNAME, nodename, PMIX_STRING);
    PMIX_INFO_LOAD(&query[0].qualifiers[0], PMIX_NSPACE, opt, PMIX_STRING);

    if (nb) {
        /* non-blocking version */
        if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(query, nq, querycbfunc, (void *) &mydata))) {
            fprintf(stderr, "Client ns %s rank %d: PMIx_Query_info failed: %d\n", myproc.nspace,
                    myproc.rank, rc);
        }
    } else {
        /* blocking version */
        PMIx_Query_info(query, nq, &res, &s);
    }

    char *res_t = PMIx_Data_type_string(res->value.type);

    // force
    res_t = "PMIX_DATA_ARRAY";

    if (0 == strcmp("PMIX_DATA_ARRAY", res_t)) {
        pmix_data_array_t *resarr = (pmix_data_array_t *) res->value.data.darray;
        for (int i = 0; i < resarr->size; i++) {
            pmix_proc_t *ppt = (pmix_proc_t *) resarr->array;
//            pmix_info_t *ppt = (pmix_info_t *) resarr->array;
//            fprintf(stderr, "Query return key: \"%s\" value: %s\n", ppt->key, ppt->value.data.string);
            fprintf(stderr, "Query return key: \"%s\" value: %d\n", ppt->nspace, ppt->rank);
        }
    } else if (0 == strcmp("PMIX_STRING", res_t)) {
        fprintf(stderr, "Query return key: \"%s\" value:%s\n", res->key, res->value.data.string);
    } else {
        fprintf(stderr, "Query return key: \"%s\" value: %s\n", res->key, res->value);
//            fprintf(stderr, "Query return key: \"%s\" value: %s\n", res->key, ppt->nspace);
    }



//    /* find the response */
//    if (PMIX_SUCCESS == mydata.lock.status ) {
//        /* should be in the first key */
//        if (PMIX_CHECK_KEY(&mydata.info[0], PMIX_SERVER_URI)) {
//            fprintf(stderr, "PMIx server URI for node %s: %s\n",
//                    (NULL == nodename) ? hostname : nodename, mydata.info[0].value.data.string);
//        } else {
//            fprintf(stderr, "Query returned wrong info key at first posn: %s\n",
//                    mydata.info[0].key);
//        }
////        fprintf(stderr, "PMIX %s\n", mydata.info[1].value.data.string);
//    } else {
//        fprintf(stderr, "Query returned error: %s\n", PMIx_Error_string(mydata.lock.status));
//    }


    PMIX_QUERY_FREE(query, nq);
    return NULL;
}

static void querycbfunc(pmix_status_t status, pmix_info_t *info, size_t ninfo, void *cbdata,
                        pmix_release_cbfunc_t release_fn, void *release_cbdata) {
    myquery_data_t *mq = (myquery_data_t *) cbdata;
    size_t n;

    mq->lock.status = status;

    /* save the returned info - it will be
     * released in the release_fn */
    if (0 < ninfo) {
        PMIX_INFO_CREATE(mq->info, ninfo);
        mq->ninfo = ninfo;
        for (n = 0; n < ninfo; n++) {
            PMIX_INFO_XFER(&mq->info[n], &info[n]);
        }
    }

    /* let the library release the data */
    if (NULL != release_fn) {
        release_fn(release_cbdata);
    }
}

static void opcbfunc(pmix_status_t status, void *cbdata)
{
    myxfer_t *x = (myxfer_t *) cbdata;

    /* release the caller, if necessary */
    if (NULL != x->cbfunc) {
        x->cbfunc(PMIX_SUCCESS, x->cbdata);
    }
    x->active = false;
}

static void setup_cbfunc(pmix_status_t status, pmix_info_t info[], size_t ninfo,
                         void *provided_cbdata, pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    myxfer_t *myxfer = (myxfer_t *) provided_cbdata;
    size_t i;

    if (PMIX_SUCCESS == status && 0 < ninfo) {
        myxfer->ninfo = ninfo;
        PMIX_INFO_CREATE(myxfer->info, ninfo);
        for (i = 0; i < ninfo; i++) {
            PMIX_INFO_XFER(&myxfer->info[i], &info[i]);
        }
    }
    if (NULL != cbfunc) {
        cbfunc(PMIX_SUCCESS, cbdata);
    }
    myxfer->active = false;
}

int main(int argc, char **argv) {

    pmix_info_t *info;
    int ninfo = 4;
    char *server_uri = NULL;
    pmix_status_t rc;
    pmix_proc_t myproc = {"UNDEF", PMIX_RANK_UNDEF};

    gethostname(hostname, 1024);
    for (int i = 0; i < argc; i++) {
        if (0 == strcmp(argv[i], "--uri")) {
            if (NULL == argv[i + 1]) {
                fprintf(stderr, "Must provide URI argumnet to %s option\n", argv[i]);
            }
            server_uri = argv[i + 1];
            ++i;
            ++ninfo;
        } else {
//            server_uri = check_uri();
        }
    }


    PMIX_INFO_CREATE(info, ninfo);
    int n = 0;
    if (server_uri != NULL) {
        PMIX_INFO_LOAD(&info[n], PMIX_SERVER_URI, server_uri, PMIX_STRING);
        fprintf(stderr, "Connect to %s\n", server_uri);
        ++n;
    }

//    bool t = 1;
    /* connect spawn launch now thread to exec*/
    PMIX_INFO_LOAD(&info[n], PMIX_TOOL_CONNECT_OPTIONAL, NULL, PMIX_BOOL);
    ++n;
    PMIX_INFO_LOAD(&info[n], PMIX_LAUNCHER, NULL, PMIX_BOOL);
    ++n;
    PMIX_INFO_LOAD(&info[n], PMIX_TOOL_NSPACE, "pmix_tool_1", PMIX_STRING);
    ++n;
    int32_t rank = 2;
    PMIX_INFO_LOAD(&info[n], PMIX_TOOL_RANK, &rank, PMIX_INT32);
    ++n;

    /* env set */
//    setenv("PMIX_NAMESPACE", "pmix_tool_ns", 1);
//    fprintf(stderr, "env is %s\n", getenv("PMIX_NAMESPACE"));

    /* init tool */
    if (PMIX_SUCCESS != (rc = PMIx_tool_init(&myproc, info, ninfo))) {
        fprintf(stderr, "PMIx tool init failed %d", rc);
        exit(rc);
    } else {
        fprintf(stderr, "PMIx tool register in nspace %s, rank %d\n", myproc.nspace, myproc.rank);
    }


    /**
     * set namespace
     * we have a single namespace for all clients */
    pmix_nspace_t jobns;
    char *tmp, **atmp;
    myxfer_t *x;

    atmp = NULL;
    int nprocs = 4;
    for (n = 0; n < nprocs; n++) {
        asprintf(&tmp, "%d", n);
        pmix_argv_append_nosize(&atmp, tmp);
        free(tmp);
    }
    tmp = pmix_argv_join(atmp, ',');
    pmix_argv_free(atmp);
    /* register the nspace */
    x = PMIX_NEW(myxfer_t);
    set_namespace(nprocs, tmp, "foobar", opcbfunc, x);
    /* if the nspace registration hasn't completed yet,
     * wait for it here */
    PMIX_WAIT_FOR_COMPLETION(x->active);
    free(tmp);
    PMIX_RELEASE(x);
//    query_info(PMIX_QUERY_NAMESPACES, "pmix_tool_1");

    PMIx_Spawn(NULL, 0, NULL, 0, jobns);
    sleep(2);
    query_info(PMIX_QUERY_PROC_TABLE, "prterun-lenovo-90556@1");

//    int cnt = 1;
//    while (1) {
//        if(cnt == 1) {
            printf("jobns is %s \n", jobns);
//        }
//        ++cnt;
//        sleep(2);
//    }

//    query_ns(myproc);
//    query_info(PMIX_SERVER_URI);
//    query_info(PMIX_QUERY_AVAIL_SERVERS);
//    query_info(PMIX_SERVER_INFO_ARRAY);


//    pmix_value_t val;
//    char *getk = PMIX_DATA_SCOPE;
//    rc = PMIx_Get(&myproc, getk, info, ninfo, &val);
//    if (rc < 0) {
//        fprintf(stderr, "PMIx_Get error is %s", PMIx_Error_string(rc));
//    } else {
//        fprintf(stderr, "Get key: \"%s\" value: \"%s\"", getk, val.data.uint8);
//    }

    pmix_nspace_t *ns;
    pmix_proc_t *oproc;
    int oprocn;
    ns = myproc.nspace;
    PMIx_Resolve_peers(hostname, ns, &oproc, &oprocn);
//
//
//    if (NULL != info) {
//        PMIX_INFO_FREE(info, ninfo);
//    }



//    query_application_namespace("prterun-lenovo-90556@10");
/* query the list of active nspaces */
    int nq = 1;
    pmix_query_t *query;
    myquery_data_t mydata;
    pmix_data_array_t *darray, *dptr;
    pmix_info_t *iptr;
    size_t m;

    PMIX_QUERY_CREATE(query, nq);
    PMIX_ARGV_APPEND(rc, query[0].keys, PMIX_QUERY_NAMESPACE_INFO);
    PMIX_QUERY_QUALIFIERS_CREATE(&query[0], 1);
    PMIX_INFO_LOAD(&query[0].qualifiers[0], PMIX_NSPACE, "prterun-lenovo-90556@1", PMIX_STRING);
    if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(query, nq, querycbfunc, (void *) &mydata))) {
        fprintf(stderr, "Client ns %s rank %d: PMIx_Query_info failed: %d\n", myproc.nspace,
                myproc.rank, rc);
        goto done;
    }
    sleep(2);
    /* find the response */
    if (PMIX_SUCCESS == mydata.lock.status) {
        /* should be in the first key */
        if (PMIX_CHECK_KEY(&mydata.info[0], PMIX_QUERY_NAMESPACE_INFO)) {
            darray = mydata.info[0].value.data.darray;
            fprintf(stderr, "ACTIVE NSPACES:\n");
            if (NULL == darray || 0 == darray->size || NULL == darray->array) {
                fprintf(stderr, "\tNone\n");
            } else {
                info = (pmix_info_t *) darray->array;
                if (NULL == info) {
                    fprintf(stderr, "Error\n");
                } else {
                    for (n = 0; n < darray->size; n++) {
                        dptr = info[n].value.data.darray;
                        if (NULL == dptr || 0 == dptr->size || NULL == dptr->array) {
                            fprintf(stderr, "Error in array %s\n",
                                    (NULL == dptr) ? "NULL" : "NON-NULL");
                            break;
                        }
                        iptr = (pmix_info_t *) dptr->array;
                        for (m = 0; m < dptr->size; m++) {
                            fprintf(stderr, "\t%s", iptr[m].value.data.string);
                        }
                        fprintf(stderr, "\n");
                    }
                }
            }
        } else {
            fprintf(stderr, "Query returned wrong info key at first posn: %s\n",
                    mydata.info[0].key);
        }
    } else {
        fprintf(stderr, "Query returned error: %s\n", PMIx_Error_string(mydata.lock.status));
    }

    done:
    /* finalize us */
    PMIx_tool_finalize();
    return rc;
}


static void set_namespace(int nprocs, char *ranks, char *nspace, pmix_op_cbfunc_t cbfunc,
                          myxfer_t *x)
{
    char *regex, *ppn;
    char hostname[PMIX_MAXHOSTNAMELEN];
    pmix_status_t rc;
    myxfer_t myxfer;
    size_t i = 0;

    gethostname(hostname, sizeof(hostname));

    /* request application setup information - e.g., network
     * security keys or endpoint info */
    PMIX_CONSTRUCT(&myxfer, myxfer_t);
    myxfer.active = true;
    if (PMIX_SUCCESS
        != (rc = PMIx_server_setup_application(nspace, NULL, 0, setup_cbfunc, &myxfer))) {
        PMIX_DESTRUCT(&myxfer);
        fprintf(stderr, "Failed to setup application: %d\n", rc);
        exit(1);
    }
    PMIX_WAIT_FOR_COMPLETION(myxfer.active);
    x->ninfo = myxfer.ninfo + 7;

    PMIX_INFO_CREATE(x->info, x->ninfo);
    if (0 < myxfer.ninfo) {
        for (i = 0; i < myxfer.ninfo; i++) {
            PMIX_INFO_XFER(&x->info[i], &myxfer.info[i]);
        }
    }
    PMIX_DESTRUCT(&myxfer);

    (void) strncpy(x->info[i].key, PMIX_UNIV_SIZE, PMIX_MAX_KEYLEN);
    x->info[i].value.type = PMIX_UINT32;
    x->info[i].value.data.uint32 = nprocs;

    ++i;
    (void) strncpy(x->info[i].key, PMIX_SPAWNED, PMIX_MAX_KEYLEN);
    x->info[i].value.type = PMIX_UINT32;
    x->info[i].value.data.uint32 = 0;

    ++i;
    (void) strncpy(x->info[i].key, PMIX_LOCAL_SIZE, PMIX_MAX_KEYLEN);
    x->info[i].value.type = PMIX_UINT32;
    x->info[i].value.data.uint32 = nprocs;

    ++i;
    (void) strncpy(x->info[i].key, PMIX_LOCAL_PEERS, PMIX_MAX_KEYLEN);
    x->info[i].value.type = PMIX_STRING;
    x->info[i].value.data.string = strdup(ranks);

    ++i;
    PMIx_generate_regex(hostname, &regex);
    (void) strncpy(x->info[i].key, PMIX_NODE_MAP, PMIX_MAX_KEYLEN);
    x->info[i].value.type = PMIX_STRING;
    x->info[i].value.data.string = regex;

    ++i;
    PMIx_generate_ppn(ranks, &ppn);
    (void) strncpy(x->info[i].key, PMIX_PROC_MAP, PMIX_MAX_KEYLEN);
    x->info[i].value.type = PMIX_STRING;
    x->info[i].value.data.string = ppn;

    ++i;
    (void) strncpy(x->info[i].key, PMIX_JOB_SIZE, PMIX_MAX_KEYLEN);
    x->info[i].value.type = PMIX_UINT32;
    x->info[i].value.data.uint32 = nprocs;

    PMIx_server_register_nspace(nspace, nprocs, x->info, x->ninfo, cbfunc, x);
}


int query_ns(pmix_proc_t myproc) {

    pmix_status_t rc;
    pmix_query_t *query;
    size_t nq;
    myquery_data_t *q;

    nq = 1;
    /* query the active nspaces so we can verify that the
     * specified one exists */
    PMIX_QUERY_CREATE(query, nq);
    PMIX_ARGV_APPEND(rc, query[0].keys, PMIX_TMPDIR);

    q = (myquery_data_t *) malloc(sizeof(myquery_data_t));
    if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(query, nq, querycbfunc, (void *) q))) {
        fprintf(stderr, "Client ns %s rank %d: PMIx_Query_info failed: %d\n", myproc.nspace,
                myproc.rank, rc);
        return -1;
    }

    sleep(2);
    if (NULL == q->info) {
        fprintf(stderr, "Query returned no info\n");
        return -1;
    }
    /* the query should have returned a comma-delimited list of nspaces */
    if (PMIX_STRING != q->info[0].value.type) {
        fprintf(stderr, "Query returned incorrect data type: %d\n", q->info[0].value.type);
        return -1;
    }
    if (NULL == q->info[0].value.data.string) {
        fprintf(stderr, "Query returned no active nspaces\n");
        return -1;
    }

    fprintf(stderr, "Query returned %s\n", q->info[0].value.data.string);
    return 0;
}



