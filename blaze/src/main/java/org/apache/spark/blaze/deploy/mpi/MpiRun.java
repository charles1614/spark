package org.apache.spark.blaze.deploy.mpi;

import org.apache.spark.blaze.JavaLoggingWrapper;

public class MpiRun extends JavaLoggingWrapper {

    public static void main(String[] args) {
        String[] app = new String[2];
        app[0] = "prun";
        app[1] = "hostname";
        MpiRun mpiRun = new MpiRun();
        mpiRun.exec(app);
    }

    public void exec(String[] args){
        int argc = args.length;
        logInfo(() -> "Prun is starting in");
        NativeUtils.mpirun(argc, args);
    }
}
