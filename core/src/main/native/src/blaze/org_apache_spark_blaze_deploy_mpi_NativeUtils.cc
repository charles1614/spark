#include <jni.h>
#include <string.h>
#include <unistd.h>
#include <map>
#include <string>
#include <utility>
#include <iostream>
#include <vector>
#include <dlfcn.h>

/* Header for class org_apache_spark_blaze_MPILauncher */
#ifndef _Included_org_apache_spark_blaze_NativeTools
#define _Included_org_apache_spark_blaze_NativeTools
#ifdef __cplusplus
extern "C" {
#endif


/* convert jstring to std::string */
std::string jstring_to_string(JNIEnv *env, jstring js) {
    const char *string = env->GetStringUTFChars(js, NULL);
    jsize length = env->GetStringLength(js);
    return std::string(string, length);
}

/*
 * Class:     org_apache_spark_blaze_NativeUtils
 * Method:    getEnv
 * Signature: ()Ljava/util/Map;
 */
JNIEXPORT jobject

JNICALL Java_org_apache_spark_blaze_deploy_mpi_NativeUtils_getEnv__
        (JNIEnv *env, jclass cls) {

    std::map<std::string, std::string> map;
    std::pair<std::string, std::string> pair;

    jclass cls_hashmap = env->FindClass("java/util/HashMap");
    jmethodID mtd_map_put = env->GetMethodID(cls_hashmap, "put",
                                             "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

    jmethodID mtd_init = env->GetMethodID(cls_hashmap, "<init>", "()V");
    jobject hashmap = env->NewObject(cls_hashmap, mtd_init);

    for (char **current = environ; *current; current++) {

        std::cout << *current << std::endl;
        pair.first = strtok(*current, "=");
//        std::cout << *current << std::endl;
        pair.second = strtok(NULL, "\n");
        std::cout << pair.first << " !=! " << pair.second << std::endl;
        map.insert(pair);
    }

    return hashmap;
}


/*
 * Class:     org_apache_spark_blaze_NativeUtils
 * Method:    getEnv
 * Signature: (Ljava/lang/String;)Ljava/util/Map;
 */
JNIEXPORT jobject

JNICALL Java_org_apache_spark_blaze_deploy_mpi_NativeUtils_getEnv__Ljava_lang_String_2
        (JNIEnv *env, jclass cls, jstring prefix) {
    std::map<std::string, std::string> map;
    std::pair<std::string, std::string> pair;


    jclass cls_hashmap = env->FindClass("java/util/HashMap");
    jmethodID mtd_map_put = env->GetMethodID(cls_hashmap, "put",
                                             "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

    jmethodID mtd_init = env->GetMethodID(cls_hashmap, "<init>", "()V");
    jobject hashmap = env->NewObject(cls_hashmap, mtd_init);


    if (NULL == environ) {
        fprintf(stderr, "environ is NULL");
        goto done;
    }

    for (char **current = environ; *current; current++) {
        if (NULL == *current) {
            goto done;
        }
//        std::cout << *current << std::endl;
        pair.first = strtok(*current, "=");
        pair.second = strtok(NULL, "\n");
//        std::cout << "s " << pair.second << std::endl;
//        std::cout << pair.first << std::endl;
        if (0 == pair.first.compare(0, env->GetStringLength(prefix), jstring_to_string(env, prefix))) {
            std::cout << pair.first << "=" << pair.second << std::endl;
            map.insert(pair);
        }

    }

    done:
    return hashmap;
}


/*
 * Class:     org_apache_spark_blaze_deploy_mpi_NativeUtils
 * Method:    loadLibrary
 * Signature: ([Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_org_apache_spark_blaze_deploy_mpi_NativeUtils_loadLibrary
        (JNIEnv *env, jclass cls, jobjectArray libs) {

    std::vector<std::string> vlibs;
    int len = env->GetArrayLength(libs);
    for (int i = 0; i < len; i++) {
        jstring lib = (jstring) env->GetObjectArrayElement(libs, i);
        const char *string = env->GetStringUTFChars(lib, NULL);
        vlibs.push_back(string);
    }
    for (auto v : vlibs) {
        auto handle = dlopen(v.c_str(), RTLD_LAZY | RTLD_GLOBAL);
        if (!handle) {
            fprintf(stderr, "%s\n", dlerror());
            exit(EXIT_FAILURE);
        }
    }

}


#ifdef __cplusplus
}
#endif


#endif
