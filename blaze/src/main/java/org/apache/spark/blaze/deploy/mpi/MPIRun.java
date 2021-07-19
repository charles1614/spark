package org.apache.spark.blaze.deploy.mpi;

import org.apache.spark.blaze.JavaLoggingWrapper;

public class MPIRun extends JavaLoggingWrapper {

    public static void main(String[] args) {
        String[] app = new String[2];
        app[0] = "prun";
        app[1] = "hostname";
        MPIRun mpiRun = new MPIRun();
        mpiRun.exec(app);
    }

    public static int launch(String[] args) {
        MPIRun mpiRun = new MPIRun();
        int rc = mpiRun.exec(args);
        return rc;
    }

    public int exec(String[] args) {
        int argc = args.length;
        // -n [num] hostname
        assert argc == 3 : "MPI job Nspace setting need 3 args: -n [num] exe";
        logInfo(() -> "MPIJob is starting with " + args[1] + "cores");
        int rc = NativeUtils.mpirun(argc, args);
        return rc;
    }
}
