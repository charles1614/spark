package org.apache.spark.blaze;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class MPILauncher extends JavaLoggingWrapper {

    //    private static final Log LOG = LogFactory.getLog(MPILauncher.class);
    public static void main(String[] args) {
        String[] argv = new String[3];
        argv[0] = "prte";
        argv[1] = "-H";
        argv[2] = "besh02";
        MPILauncher mpiLauncher = new MPILauncher();
        mpiLauncher.mpiRTE(argv);
    }

    public void mpiRTE(String[] argv) {
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
        NativeUtils.rte(argc, argv);
    }
}
