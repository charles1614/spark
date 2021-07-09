package org.apache.spark.blaze;

import java.util.Map;

public class NativeUtils {
    static {
        System.load(
"/home/xialb/opt/spark/blaze/src/main/native/jni/cmake-build-debug/libblaze.so");
    }

    public static native int rte(int argc, String[] argv);
    public static native int mpirun(int argc, String[] argv);
    public static native int test();
    public static native int setEnv(Map<String, String> m);
    public static native Map<String, String> getEnv();
    public static native Map<String, String> getEnv(String prefix);
}
