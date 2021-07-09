
package test.org.apache.spark.blaze

import mpi.{MPI, MPIException}
import org.apache.spark.SparkConf
import org.apache.spark.blaze.NativeUtils.{getEnv, setEnv, test}
import org.apache.spark.blaze.{BlazeSession, BlazeUtils, NativeUtils}

import java.lang.Thread.sleep
import java.util
import scala.math.random

/** Computes an approximation to pi */
object SparkPi {

  @throws[MPIException]
  private def mpiop(mpargs: Array[String]): Unit = {
    val map = new util.HashMap[String, String]()

    map.put("PMIX_RANK", "1")
    map.put("PMIX_NAMESPACE", "yes")
    map.put("PMIX_SERVER_URI41", "prte-lenovo-226447@0.0tcp4://172.16.0.1:37929")
    map.put("PMIX_SERVER_URI4", "prte-lenovo-226447@0.0;tcp4://172.16.0.1:37929")
//    map.put("PMIX_SERVER_URI3", "prte-lenovo-226447@0.0;tcp4://172.16.0.1:37929")
//    map.put("PMIX_SERVER_URI2", "prte-lenovo-226447@0.0;tcp4://172.16.0.1:37929")
//    map.put("PMIX_SERVER_URI21", "prte-lenovo-226447@0.0;tcp4://172.16.0.1:37929")
    map.put("PMIX_SERVER_TMPDIR", "/tmp/prte.lenovo.1000/dvm.226447")
    //    map.put("PMIX_")
    BlazeUtils.setJavaEnv(map)
//    print(sys.env)
    setEnv(map)
//    getEnv()

    MPI.InitThread(mpargs, MPI.THREAD_MULTIPLE)
    getEnv()
//    while (true) {
//      sleep(1000)
//    };
    val myrank = MPI.COMM_WORLD.getRank
    val size = MPI.COMM_WORLD.getSize
    getEnv()
    print("Hello world from rank " + myrank + " size of " + size + "\n")
    MPI.Finalize()
    //    NativeUtils.test()

  }

  System.load(
    "/home/xialb/opt/spark/blaze/src/main/native/jni/cmake-build-debug/libblaze.so")

  def main(args: Array[String]): Unit = {

    mpiop(args);

    val conf = new SparkConf().set("spark.master", "spark://192.168.32.197:7077");

    val stringToString = new util.HashMap[String, String]()
    stringToString.put("key", "value")
    stringToString.put("key1", "value1")

    val blaze = BlazeSession
      .builder
      .appName("blazePi")
      //      .config(conf)
      .master("local[*]")
      //      .master("spark://192.168.32.197:7077")
      .getOrCreate()

    val slices = if (args.length > 0) args(0).toInt else 2
    //    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    //    val count = blaze.sparkContext.parallelize(1 until n, slices).map { i =>
    //      //      mpiop(args)
    //      val x = random * 2 - 1
    //      val y = random * 2 - 1
    //      if (x * x + y * y <= 1) 1 else 0
    //    }.reduce(_ + _)

    //    blaze.sparkContext.parallelize(1 until 3, slices).map(i => mpiop(args)).collect()
    blaze.mpiContext.parallelize(1 until 2, slices).map(i => mpiop(args)).collect()
    //    print(s"Pi is roughly ${4.0 * count / (n - 1)} \n")
    blaze.stop()
  }
}
