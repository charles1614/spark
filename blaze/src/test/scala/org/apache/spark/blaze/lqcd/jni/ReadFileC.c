#include <jni.h>
#include <stdlib.h>
#include <init.h>
#include <linalg.h>
#include <dirac.h>
#include <test.h>

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_apache_spark_examples_lqcd_ReadFileC
 * Method:    readGaugeFiledC
 * Signature: (Ljava/lang/String;)Lorg/apache/spark/examples/lqcd/SU3Field
 */
JNIEXPORT jobject JNICALL Java_org_apache_spark_examples_lqcd_ReadFileC_readGaugeFiledC
        (JNIEnv *env, jobject obj, jstring path) {

    const jbyte *filename = (*env)->GetStringUTFChars(env, path, NULL);

    // get the java class SU3Field
    jclass ucls = (*env)->FindClass(env, "org/apache/spark/examples/lqcd/SU3Field");
    // constructor method
    jmethodID cst = (*env)->GetMethodID(env, ucls, "<init>", "(I)V");
//    jmethodID ini = (*env)->GetMethodID(env, ucls, "randomSU3", "()Lorg/apache/spark/examples/lqcd/SU3");
//    jmethodID init = (*env)->GetMethodID(env, ucls, "initGaugeRandom", "(I)Lorg/apache/spark/examples/lqcd/SU3Field");
//    if (init == NULL){
//        printf("not found");
//        exit(1);
//    }
    // su3field obj
    jobject u = (*env)->NewObject(env, ucls, cst, 2);
//    jobject pJobject = (*env)->CallObjectMethod(env, u, init, 1);

    // get the field id
    jfieldID suf3Id = (*env)->GetFieldID(env, ucls, "su3Field", "[[[[[Lorg/apache/spark/examples/lqcd/SU3;");
    jobjectArray su3f = (*env)->GetObjectField(env, u, suf3Id);

    jmethodID readGfield = (*env)->GetMethodID(env, ucls, "readGaugeField","(D)I");
    double v = 0.234;
    int a = 1;
    a = (*env)->CallObjectMethod(env, u, readGfield, v);
    if (a == 1){
        printf("call method failed");
        exit(1);
    }


    jclass su3 = (*env)->FindClass(env, "org/apache/spark/examples/lqcd/SU3");


//    jint i = (*env)->GetIntField(env, u, pId);

//    printf("%d", i);
    jclass cls = (*env)->GetObjectClass(env, u);
    if (cls == NULL) {
        printf("cls failed\n");
        exit(1);
    }


//    su3_field *data;
//    su3_field_alloc(&data);
//    printf("size of su3_field = %i\n", sizeof(data));
//
//    double db[100];
//    FILE *pFile = fopen(filename, "rb");
//    fseek(pFile, 0, SEEK_END);
//    long size = ftell(pFile);
//    rewind(pFile);
//
//    double *pDouble = (double *) malloc(sizeof(double) * 10);
//    fread(db, 1, 100, pFile);
//    free(pDouble);
//
//    read_gauge_field(data, filename);
//    // I need to swap endianess on my machine -_-
//    swap_endian(data);
//
//    printf("gauge field readed: \n");
//    printf("%f\n", (((*data)[0][0][0][0][0]).c11.re));
//    su3_field_free(&data);
//
//
//    (*env)->ReleaseStringUTFChars(env, filename, NULL);
    return u;
}

#ifdef __cplusplus
}
#endif