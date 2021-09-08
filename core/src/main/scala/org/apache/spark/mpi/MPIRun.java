package org.apache.spark.mpi;

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
        int rc = NativeUtil.mpirun(argc, args);
        return rc;
    }

}
