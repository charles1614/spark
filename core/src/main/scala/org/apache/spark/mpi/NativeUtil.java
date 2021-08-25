
package org.apache.spark.mpi;

import java.util.Map;

public class NativeUtil {

  public  static native int namespaceFinalize(String namespace);

  public static native String namespaceQuery();

  public static native int setEnv(Map<String, String> m);

  public static native int mpirun(int argc, String[] argv);
}
