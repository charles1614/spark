package org.apache.spark.mpi;

import org.junit.Test;

import static java.lang.Thread.sleep;

public class MPIUtilSuite {

    @Test
    public void mpirun() {
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
