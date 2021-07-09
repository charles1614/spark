#include <jni.h>
#include <string.h>
#include "prte.h"
#include "prun.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <map>
#include <string>
#include <utility>
#include <iostream>

//#include "prte.c"
/* Header for class org_apache_spark_blaze_MPILauncher */
#ifndef _Included_org_apache_spark_blaze_NativeTools
#define _Included_org_apache_spark_blaze_NativeTools
#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_apache_spark_blaze_NativeUtils
 * Method:    getEnv
 * Signature: ()Ljava/util/Map;
 */
JNIEXPORT jobject JNICALL Java_org_apache_spark_blaze_NativeUtils_getEnv__
        (JNIEnv *env, jclass cls) {

    std::map<std::string, std::string> map;
    std::pair<std::string, std::string> pair;

    jclass cls_hashmap = env->FindClass("java/util/HashMap");
    jmethodID mtd_map_put = env->GetMethodID(cls_hashmap, "put",
                                             "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

    jmethodID mtd_init = env->GetMethodID(cls_hashmap, "<init>", "()V");
    jobject hashmap = env->NewObject(cls_hashmap, mtd_init);

    for (char **current = environ; *current; current++) {
        pair.first = strsep(current, "=");
        pair.second = strsep(current, "=");
        std::cout << pair.first << "=" << pair.second << std::endl;
//        map.insert(pair);
    }

    return hashmap;
}


/*
 * Class:     org_apache_spark_blaze_NativeUtils
 * Method:    getEnv
 * Signature: (Ljava/lang/String;)Ljava/util/Map;
 */
JNIEXPORT jobject JNICALL Java_org_apache_spark_blaze_NativeUtils_getEnv__Ljava_lang_String_2
        (JNIEnv *env, jclass cls, jstring prefix){
    std::map<std::string, std::string> map;
    std::pair<std::string, std::string> pair;

    jclass cls_hashmap = env->FindClass("java/util/HashMap");
    jmethodID mtd_map_put = env->GetMethodID(cls_hashmap, "put",
                                             "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

    jmethodID mtd_init = env->GetMethodID(cls_hashmap, "<init>", "()V");
    jobject hashmap = env->NewObject(cls_hashmap, mtd_init);

    for (char **current = environ; *current; current++) {
        pair.first = strsep(current, "=");
        pair.second = strsep(current, "=");
        std::cout << pair.first << "=" << pair.second << std::endl;
//        map.insert(pair);
    }

    return hashmap;
}

#ifdef __cplusplus
}
#endif


#endif
