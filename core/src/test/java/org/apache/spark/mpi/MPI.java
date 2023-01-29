

import org.apache.spark.mpi.MPIRun;
import org.apache.spark.blaze.deploy.mpi.NativeUtils;
import org.apache.spark.blaze.deploy.mpi.MPILauncher;
import static java.lang.Thread.sleep;

public class MPI {

  public static void main(String[] args) {

    // String[] argv = new String[3];
    // argv[0] = "prte";
    // argv[1] = "-H";
    // argv[2] = "besh02:8";
    //
    // String[] strings = new String[1];
    // strings[0] = "/home/xialb/git/spark/lib/libblaze.so";
    // NativeUtils.loadLibrary(strings);
    // MPILauncher mpiLauncher = new MPILauncher();
    // mpiLauncher.mpiRTE(argv);

    // String ns = NativeUtils.namespaceQuery();
    // System.out.println(ns);
    // String[] ns_arr = ns.split(",");
    // System.out.println(ns);
    // int ret = 2;
    // for (String n : ns_arr) {
    //   if (n.equals("")){
    //     ret = 1;
    //     break;
    //   }
    //   ret = NativeUtils.namespaceFinalize(n);
    //   System.out.println("Finalize Namespace " + n);


            String[] app = new String[6];
            app[0] = "prun";
            app[1] = "-n";
            app[2] = "3";
            app[3] = "--map-by";
            app[4] = ":OVERSUBSCRIBE";
            app[5] = "hostname";
            System.load(System.getenv("SPARK_HOME") + "/lib/libblaze.so");
    //        Thread t = new Thread(() -> {
            MPIRun mpiRun = new MPIRun();
            mpiRun.exec(app);
    //        });
    //        t.start();
            try {
                sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.exit(0);
    //        assert (t.getState() == Thread.State.RUNNABLE);
  }
}
