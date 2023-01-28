

// import org.apache.spark.mpi.MPIRun;
import org.apache.spark.blaze.deploy.mpi.NativeUtils;
import static java.lang.Thread.sleep;

public class MPI {

  public static void main(String[] args) {

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
    }

    //         String[] app = new String[6];
    //         app[0] = "prun";
    //         app[1] = "-n";
    //         app[2] = "3";
    //         app[3] = "--map-by";
    //         app[4] = ":OVERSUBSCRIBE";
    //         app[5] = "hostname";
    //         System.load(System.getenv("SPARK_HOME") + "/lib/libblaze.so");
    // //        Thread t = new Thread(() -> {
    //         MPIRun mpiRun = new MPIRun();
    //         mpiRun.exec(app);
    // //        });
    // //        t.start();
    //         try {
    //             sleep(2000);
    //         } catch (InterruptedException e) {
    //             e.printStackTrace();
    //         }
    //         System.exit(0);
    //        assert (t.getState() == Thread.State.RUNNABLE);
  }
}
