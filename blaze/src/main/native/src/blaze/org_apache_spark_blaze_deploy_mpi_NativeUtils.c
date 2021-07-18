#include <jni.h>
#include <string.h>
#include "prte.h"
#include "prun.h"
#include <stdlib.h>
#include <stdio.h>

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
JNIEXPORT jint JNICALL Java_org_apache_spark_blaze_deploy_mpi_NativeUtils_setEnv
        (JNIEnv *env, jclass cls, jobject hashmap) {
    jclass cls_hashmap = (*env)->GetObjectClass(env, hashmap);

    /* get map-key */
    jmethodID mtd_keyset =
            (*env)->GetMethodID(env, cls_hashmap, "keySet", "()Ljava/util/Set;");
    if (mtd_keyset == NULL) {
        printf("keySet not found\n");
        return -1;
    }
    jobject keyset = (*env)->CallObjectMethod(env, hashmap, mtd_keyset);
    jclass cls_set = (*env)->GetObjectClass(env, keyset);

    jmethodID mtd_to_array =
            (*env)->GetMethodID(env, cls_set, "toArray", "()[Ljava/lang/Object;");
    if (mtd_to_array == NULL) {
        printf("to Array not found\n");
        return -2;
    }
    jobjectArray array_of_keys =
            (*env)->CallObjectMethod(env, keyset, mtd_to_array);
    int array_size = (*env)->GetArrayLength(env, array_of_keys);

    for (int i = 0; i < array_size; i++) {
        jstring key = (*env)->GetObjectArrayElement(env, array_of_keys, i);
        const char *c_string_key = (*env)->GetStringUTFChars(env, key, 0);
        jmethodID mtd_get =
                (*env)->GetMethodID(env,
                                    cls_hashmap,
                                    "get",
                                    "(Ljava/lang/Object;)Ljava/lang/Object;");
        if (mtd_get == NULL) {
            return -3;
        }
        jobject value = (*env)->CallObjectMethod(env, hashmap, mtd_get, key);
        const char *c_string_value = (*env)->GetStringUTFChars(env, value, 0);
        setenv(c_string_key, c_string_value, 1);
//        printf("[key, value]=[%s, %s]\n", c_string_key, c_string_value);

        /* release local stuff*/
        (*env)->ReleaseStringUTFChars(env, key, c_string_key);
        (*env)->DeleteLocalRef(env, key);

        (*env)->ReleaseStringUTFChars(env, value, c_string_value);
        (*env)->DeleteLocalRef(env, value);
    }
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
