package org.apache.spark.blaze.deploy.mpi;

import org.apache.spark.blaze.JavaLoggingWrapper;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class MPILauncher extends JavaLoggingWrapper {

    //    private static final Log LOG = LogFactory.getLog(MPILauncher.class);
    public static void main(String[] args) {
        String[] argv = new String[3];
        argv[0] = "prte";
        argv[1] = "-H";
        argv[2] = "lenovo:2";

        String[] strings = new String[1];
        strings[0] = "/home/xialb/lib/libblaze.so";
        NativeUtils.loadLibrary(strings);
        MPILauncher mpiLauncher = new MPILauncher();
        mpiLauncher.mpiRTE(argv);
    }

    public static int launch(String[] hosts) {
        MPILauncher mpiLauncher = new MPILauncher();
        int rc = mpiLauncher.mpiRTE(hosts);
        return rc;
    }

    public int mpiRTE(String[] argv) {
        int argc = argv.length;
        logInfo(() -> {
            try {
                return "MPI RunTimeEnv is running in host "
                        + InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                e.printStackTrace();
                return "Unknown host";
            }
        });
        int rc = NativeUtils.rte(argc, argv);
        return rc;
    }
}
