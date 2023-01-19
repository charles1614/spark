package org.apache.spark.blaze.deploy.mpi;

import java.util.Map;

public class NativeUtils {
    static {
        System.load(System.getenv("SPARK_HOME") + "/lib/libblaze.so");

    }

    public static native int rte(int argc, String[] argv);

    public static native int mpirun(int argc, String[] argv);

    public static native int test();

    public static native int setEnv(Map<String, String> m);

    public static native Map<String, String> getEnv();

    public static native Map<String, String> getEnv(String prefix);

    public static native int namespaceFinalize(String namespace);

    public static native String namespaceQuery();

    public static native void loadLibrary(String[] argv);
}
