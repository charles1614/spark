
package org.apache.spark.mpi;

import java.util.Map;

public class NativeUtil {

  public  static native int namespaceFinalize(String namespace);

  public native static String namespaceQuery();

  public static native int setEnv(Map<String, String> m);
}
