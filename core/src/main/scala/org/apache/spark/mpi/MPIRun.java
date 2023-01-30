package org.apache.spark.mpi;

// import java.io.IOException;

public class MPIRun extends JavaLoggingWrapper {

  public static int launch(String[] args) {
    MPIRun mpiRun = new MPIRun();
    int rc = mpiRun.exec(args);
    return rc;
  }

  public int exec(String[] args) {
    int argc = args.length;
    // -n [num] hostname
    if (argc != 6) {
      throw new RuntimeException("MPI job Nssdpace setting need 3 args: -n [num] exe");
    }
    logInfo(() -> "MPIJob is starting with " + args[4] + " cores");
    // TODO: I DON'T KNOW WHY, BUT NOT WORKING in 2023!
    int rc = NativeUtil.mpirun(argc, args);
    // ProcessBuilder pb = new ProcessBuilder(args);
    // try {
    // Process process = pb.start();
    // } catch (IOException e) {
    // throw new RuntimeException(e);
    // }
    return 0;
  }

}
