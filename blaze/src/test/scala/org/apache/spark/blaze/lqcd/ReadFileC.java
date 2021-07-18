
// scalastyle:off println
package org.apache.spark.blaze.lqcd;

public class ReadFileC {
    static {
        System.load("/home/xia/opt/lqcd/lib/libread_file_c.so");
//        System.loadLibrary("ReadFileC");
    }
    public native static SU3Field readGaugeFiledC(String path);
}
