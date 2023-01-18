package org.apache.spark.blaze.deploy.mpi;

//import org.junit.Test;

//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;

public class NativeUtilsTest {

//  @Test
//    public void testNamespaceFinalize() {
//        String ns = NativeUtils.namespaceQuery();
//        System.out.println(ns);
//        String[] ns_arr = ns.split(",");
//        System.out.println(ns);
//        int ret = 2;
//        for (String n : ns_arr) {
//            if (n.equals("")){
//                ret = 1;
//                break;
//            }
//            ret = NativeUtils.namespaceFinalize(n);
//            System.out.println("Finalize Namespace " + n);
//        }
//        assertEquals(1, ret);
//    }
//
//    //    @Test
//    public void testNamespaceQuery() throws InterruptedException {
////        new Thread(() -> mpirun()).start();
//        String ns = NativeUtils.namespaceQuery();
//        String[] ns_arr = ns.split(",");
//        int ret = 1;
//        for (String n : ns_arr) {
//            if (n.equals("")){
//                ret = 0;
//                break;
//            }
//            ret = NativeUtils.namespaceFinalize(n);
//            System.out.println("Finalize Namespace " + n);
//        }
//        System.out.println(ns_arr[0]);
////        Thread.sleep();
//        assertTrue(ns.startsWith("prte"));
//    }
//
//    public void teatRte() {
//            String[] argv = new String[3];
//            argv[0] = "prte";
//            argv[1] = "-H";
//            argv[2] = "lenovo:8";
//
//            String[] strings = new String[1];
//            strings[0] = "/home/xialb/lib/libblaze.so";
//            NativeUtils.loadLibrary(strings);
//            MPILauncher mpiLauncher = new MPILauncher();
//            mpiLauncher.mpiRTE(argv);
//    }
//    @Test
//    public void mpirun() {
//        String[] app = new String[4];
//        app[0] = "prun";
//        app[1] = "-n";
//        app[2] = "2";
//        app[3] = "hostname";
//        MPIRun mpiRun = new MPIRun();
//        int rc = mpiRun.exec(app);
//        assert(rc == 0);
//    }

    public static void main(String[] args) {
        String[] app = new String[4];
        app[0] = "prun";
        app[1] = "-n";
        app[2] = "2";
        app[3] = "hostname";
        MPIRun mpiRun = new MPIRun();
        int rc = mpiRun.exec(app);
        assert (rc == 0);
    }
}
