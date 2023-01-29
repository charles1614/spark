package org.apache.spark.blaze.deploy.mpi;

import org.apache.spark.blaze.JavaLoggingWrapper;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class MPILauncher extends JavaLoggingWrapper {

    public static int launch(String[] hosts) {
        MPILauncher mpiLauncher = new MPILauncher();
        for(int a:hosts)
          System.out.println(a);
        int rc = mpiLauncher.mpiRTE(hosts);
        return rc;
    }

    public int mpiRTE(String[] argv) {
        int argc = argv.length;
        logInfo(() -> {
            try {
                return "MPI RunTimeEnv is running in host "
                        + InetAddress.getLocalHost().getHostName() + " hosts: " + argv;
            } catch (UnknownHostException e) {
                e.printStackTrace();
                return "Unknown host";
            }
        });
        int rc = NativeUtils.rte(argc, argv);
        return rc;
    }
}
