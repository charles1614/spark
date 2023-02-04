

import org.apache.spark.blaze.deploy.mpi.MPIRun;
import org.apache.spark.blaze.deploy.mpi.NativeUtils;
import org.apache.spark.blaze.deploy.mpi.MPILauncher;
import static java.lang.Thread.sleep;


public class MPI {

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
      NativeUtils.namespaceFinalize(n);
      System.out.println("Finalize Namespace " + n);
    }
  }

  public static void process() {
    ProcessBuilder pb = new ProcessBuilder("java", "-cp", "/home/xialb/git/spark/core/src/test/java/org/apache/spark/mpi:/home/xialb/git/spark/examples/target/scala-2.12/jars/*:/home/xialb/git/spark/assembly/target/scala-2.12/jars/*", "Q" );
    pb.inheritIO();
    try {
      Process p = pb.start();
      p.waitFor();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public static void prun() {
    ProcessBuilder pb = new ProcessBuilder("prun", "-n", "1", "hostname" );
    pb.inheritIO();
    try {
      Process p = pb.start();
      p.waitFor();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  public static void main(String[] args) {

      System.out.println("pid mian: " + ProcessHandle.current().pid());
    // String[] argv = new String[3];
    // argv[0] = "prte";
    // argv[1] = "-H"; argv[2] = "besh02:8";
    // String[] strings = new String[1];
    // strings[0] = "/home/xialb/git/spark/lib/libblaze.so";
    // NativeUtils.loadLibrary(strings);
    // MPILauncher mpiLauncher = new MPILauncher();
    // mpiLauncher.mpiRTE(argv);

    String[] app = new String[4];
    app[0] = "prun";
    app[1] = "-n";
    app[2] = "3";
    // app[3] = "--map-by";
    // app[4] = ":OVERSUBSCRIBE";
    app[3] = "hostname";
    System.load(System.getenv("SPARK_HOME") + "/lib/libblaze.so");

    Thread t = new Thread(() -> {
      prun();
    });
    t.start();
    try {
      sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    f();

    // Thread t1 = new Thread(() -> {
    //   f();
    // });
    //
    // t1.start();
    // try {
    //   t1.join();
    // } catch (InterruptedException e) {
    //   e.printStackTrace();
    // }
    //
    try {
      sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }


    System.out.println("Main Thread");

    Thread t3 = new Thread(() -> {
      prun();
    });
    t3.start();
    try {
      sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Thread t2= new Thread(() -> {
    //   f();
    // });
    // t2.start();
    // try {
    //   t2.join();
    // } catch (InterruptedException e) {
    //   e.printStackTrace();
    // }
    //

    // try{
    //   t3.join();
    // }catch (Exception e){
    //   e.printStackTrace();
    // }
    //
    f();

    System.exit(0);
    //        assert (t.getState() == Thread.State.RUNNABLE);
  }
}
