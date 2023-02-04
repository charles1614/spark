#include <jni.h>
#include <string.h>
#include "prte.h"
#include "prun.h"
#include "src/pmix/pmix-internal.h"
#include <pmix_common.h>
#include <pmix.h>
#include <stdlib.h>
#include <stdio.h>
#include <src/runtime/runtime.h>
#include <src/runtime/prte_globals.h>
#include "pmix_internal.h"

//#include "prte.c"
/* Header for class org_apache_spark_blaze_MPILauncher */
#ifndef _Included_org_apache_spark_blaze_NativeTools
#define _Included_org_apache_spark_blaze_NativeTools
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_apache_spark_blaze_NativeTools
 * Method:    rte
 * Signature: (I[Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_org_apache_spark_blaze_deploy_mpi_NativeUtils_rte
        (JNIEnv *env, jclass cls, jint argc, jobjectArray argv) {
    char *pargv[argc + 1];

    int stringCount = (*env)->GetArrayLength(env, argv);

    for (int i = 0; i < stringCount; i++) {
        jstring string = (*env)->GetObjectArrayElement(env, argv, i);
        pargv[i] = (*env)->GetStringUTFChars(env, string, NULL);
    }
    pargv[argc + 1] = NULL;
    prte(argc, pargv);
}

/*
 * Class:     org_apache_spark_blaze_NativeTools
 * Method:    mpirun
 * Signature: (I[Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_org_apache_spark_blaze_deploy_mpi_NativeUtils_mpirun
        (JNIEnv *env, jclass cls, jint argc, jobjectArray argv) {
    char *pargv[argc + 1];
    int stringCount = (*env)->GetArrayLength(env, argv);
    for (int i = 0; i < stringCount; ++i) {
        jstring string = (*env)->GetObjectArrayElement(env, argv, i);
        pargv[i] = (*env)->GetStringUTFChars(env, string, NULL);
    }
    pargv[argc] = NULL;
    prun(argc, pargv);
}

extern char **environ;

JNIEXPORT jint JNICALL Java_org_apache_spark_blaze_deploy_mpi_NativeUtils_test
        (JNIEnv *env, jclass cls) {
    for (char **current = environ; *current; current++) {
        puts(*current);
    }
}


/*
 * Class:     org_apache_spark_blaze_NativeUtils
 * Method:    setEnv
 * Signature: (Ljava/util/Map;)I
 */
//JNIEXPORT jint JNICALL Java_org_apache_spark_blaze_deploy_mpi_NativeUtils_setEnv
//        (JNIEnv *env, jclass cls, jobject hashmap) {
//
//    printf("hello world/n");
//
//    jclass cls_hashmap = (*env)->GetObjectClass(env, hashmap);
//
//    /* get map-key */
//    jmethodID mtd_keyset =
//            (*env)->GetMethodID(env, cls_hashmap, "keySet", "()Ljava/util/Set;");
//    if (mtd_keyset == NULL) {
//        printf("keySet not found\n");
//        return -1;
//    }
//    jobject keyset = (*env)->CallObjectMethod(env, hashmap, mtd_keyset);
//    jclass cls_set = (*env)->GetObjectClass(env, keyset);
//
//    jmethodID mtd_to_array =
//            (*env)->GetMethodID(env, cls_set, "toArray", "()[Ljava/lang/Object;");
//    if (mtd_to_array == NULL) {
//        printf("to Array not found\n");
//        return -2;
//    }
//    jobjectArray array_of_keys =
//            (*env)->CallObjectMethod(env, keyset, mtd_to_array);
//    int array_size = (*env)->GetArrayLength(env, array_of_keys);
//
//    for (int i = 0; i < array_size; i++) {
//        jstring key = (*env)->GetObjectArrayElement(env, array_of_keys, i);
//        const char *c_string_key = (*env)->GetStringUTFChars(env, key, 0);
//        jmethodID mtd_get =
//                (*env)->GetMethodID(env,
//                                    cls_hashmap,
//                                    "get",
//                                    "(Ljava/lang/Object;)Ljava/lang/Object;");
//        if (mtd_get == NULL) {
//            return -3;
//        }
//        jobject value = (*env)->CallObjectMethod(env, hashmap, mtd_get, key);
//        const char *c_string_value = (*env)->GetStringUTFChars(env, value, 0);
//        setenv(c_string_key, c_string_value, 1);
////        printf("[key, value]=[%s, %s]\n", c_string_key, c_string_value);
//
//        /* release local stuff*/
//        (*env)->ReleaseStringUTFChars(env, key, c_string_key);
//        (*env)->DeleteLocalRef(env, key);
//
//        (*env)->ReleaseStringUTFChars(env, value, c_string_value);
//        (*env)->DeleteLocalRef(env, value);
//    }
//
//
//    // TODO: DELET ME
//pathvar = getenv("PMIX_NAMESPACE");
//printf("pathvar=%s\n",pathvar);
//    return 0;
//}

/*
 * Class:     org_apache_spark_blaze_deploy_mpi_NativeUtils
 * Method:    namespaceQuery
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_apache_spark_blaze_deploy_mpi_NativeUtils_namespaceQuery
        (JNIEnv *env, jclass cls) {

    int rc = PRTE_ERR_FATAL;

    static pmix_proc_t myproc;
    pmix_info_t *iptr;
    size_t ninfo;
    int ret = PRTE_ERROR;

    /* init the tiny part of PRTE we use */
    prte_init_util(PRTE_PROC_MASTER);

    /* now initialize PMIx - we have to indicate we are a launcher so that we
     * will provide rendezvous points for tools to connect to us */
    if (PMIX_SUCCESS != (ret = PMIx_tool_init(&myproc, NULL, NULL))) {
        fprintf(stderr, "%s failed to initialize, likely due to no DVM being available\n",
                prte_tool_basename);
        exit(1);
    }

    myquery_data_t myquery_data;


    size_t nq = 1;
    pmix_query_t *query;
    PMIX_QUERY_CREATE(query, nq);
    PMIX_ARGV_APPEND(rc, query[0].keys, PMIX_QUERY_NAMESPACES);
    /* setup the caddy to retrieve the data */
    // DEBUG_CONSTRUCT_LOCK(&myquery_data.lock);
    myquery_data.info = NULL;
    myquery_data.ninfo = 0;
    /* execute the query */
    if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(query, nq, cbfunc, (void *) &myquery_data))) {
        fprintf(stderr, "PMIx_Query_info failed: %d\n", rc);
    }
    // DEBUG_WAIT_THREAD(&myquery_data.lock);
    // DEBUG_DESTRUCT_LOCK(&myquery_data.lock);
    sleep(2);
    char *nspace = myquery_data.info->value.data.string;
    jstring jnspace = (*env)->NewStringUTF(env, nspace);
    return jnspace;
}

/*
 * Class:     org_apache_spark_blaze_deploy_mpi_NativeUtils
 * Method:    namespaceFinalize
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT int JNICALL Java_org_apache_spark_blaze_deploy_mpi_NativeUtils_namespaceFinalize
        (JNIEnv *env, jclass cls, jstring ns) {

    const char *namespace = (*env)->GetStringUTFChars(env, ns, 0);

    prte_pmix_lock_t lock;

    pmix_proc_t *target;
    pmix_info_t directive;

    PMIX_PROC_CREATE(target, 1);
    strcpy(target->nspace, namespace);
    target->rank = PMIX_RANK_WILDCARD;
    PMIX_INFO_LOAD(&directive, PMIX_JOB_CTRL_KILL, NULL, PMIX_BOOL);

    PMIx_Job_control_nb(target, 1, &directive, 1, infocb, (void *) &lock);
    sleep(2);
    (*env)->ReleaseStringUTFChars(env, ns, namespace);
    return 0;
}

/*
 * Class:     org_apache_spark_blaze_NativeUtils
 * Method:    getEnv
 * Signature: ()Ljava/util/Map;
 */
//JNIEXPORT jobject JNICALL Java_org_apache_spark_blaze_deploy_mpi_NativeUtils_getEnv
//        (JNIEnv *env, jclass cls) {
//
//    jclass cls_hashmap = (*env)->FindClass(env, "java/util/HashMap");
//    jmethodID mtd_map_put = (*env)->GetMethodID(env, cls_hashmap, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
//
//
//    for (char **current = environ; *current; current++) {
//        puts(*current);
//    }
//
//}

#ifdef __cplusplus
}
#endif


#endif
