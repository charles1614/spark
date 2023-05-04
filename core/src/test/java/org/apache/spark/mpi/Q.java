

import org.apache.spark.blaze.deploy.mpi.MPIRun;
import org.apache.spark.blaze.deploy.mpi.NativeUtils;
import org.apache.spark.blaze.deploy.mpi.MPILauncher;
import static java.lang.Thread.sleep;

public class Q {

  public static void f() {
    String ns = NativeUtils.namespaceQuery();
    System.out.println(ns);
    String[] ns_arr = ns.split(",");
    System.out.println(ns);
    int ret = 2;
    for (String n : ns_arr) {
      if (n.equals("")){
        ret = 1;
        break;
      }
      ret = NativeUtils.namespaceFinalize(n);
      System.out.println("Finalize Namespace " + n);
      break;
    }
  }

  public static void main(String[] args) {

    System.out.println("pid FucntionHost: " + ProcessHandle.current().pid());
    // String[] argv = new String[3];
    // argv[0] = "prte";
    // argv[1] = "-H"; argv[2] = "besh02:8";
    // String[] strings = new String[1];
    // strings[0] = "/home/xialb/git/spark/lib/libblaze.so";
    // NativeUtils.loadLibrary(strings);
    // MPILauncher mpiLauncher = new MPILauncher();
    f();
  }
 

}
