package org.apache.spark.mpi;

import org.junit.Test;
import scala.xml.Null;

import static java.lang.Thread.sleep;

public class MPIUtilSuite {

    @Test
    public void mpirun() {
        String[] app = new String[4];
        app[0] = "prun";
        app[1] = "-n";
        app[2] = "2";
        app[3] = "hostname";
        System.load("/home/xialb/lib/libblaze.so");
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
