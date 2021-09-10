package org.apache.spark.examples.blaze;

import mpi.MPI;
import mpi.MPIException;
import org.apache.commons.lang3.time.StopWatch;

import java.util.Random;

public class JavaMPIPi {
    public static void main(String[] args) throws MPIException {

        StopWatch watch = new StopWatch();

        watch.start();
        MPI.Init(args);
        int myrank = MPI.COMM_WORLD.getRank();
        int size = MPI.COMM_WORLD.getSize();
        int points = Integer.MAX_VALUE;
        int ppn = 0;
        if (myrank != 0) {
            ppn = points / size;
        } else {
            ppn = points - (points / size) * (size - 1);
        }
        int cnt = 0;
        Random random = new Random(myrank);
        for (int i = 0; i < ppn ; i++) {
            double x = random.nextDouble() * 2 - 1;
            double y = random.nextDouble() * 2 - 1;
            if (x * x + y * y <= 1) {
                cnt += 1;
            }
        }
        int tag = 50;
        int[] send = new int[]{cnt};
        int[] recv = new int[]{cnt};
        MPI.COMM_WORLD.send(send, 1, MPI.INT, 0, 50);
        MPI.COMM_WORLD.reduce(send, recv, 1, MPI.INT, MPI.SUM, 0);

        if (myrank == 0) {
            System.out.println((double)recv[0] / points * 4);
        }
        watch.stop();
        System.out.println("Time Elapsed: " + watch.getTime()); // Prints: Time Elapsed: 2501

        MPI.Finalize();
    }
}
