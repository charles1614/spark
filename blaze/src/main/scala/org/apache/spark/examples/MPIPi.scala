
// scalastyle:off println
package org.apache.spark.examples

import mpi.{Comm, MPI, MPIException}
import org.apache.spark.SparkConf
import org.apache.spark.blaze.deploy.mpi.NativeUtils
import org.apache.spark.blaze.ompi.MPIConf
import org.apache.spark.blaze.{BlazeSession, BlazeUtils}

import java.util
import scala.math.random
import scala.util.Random


/** Computes an approximation to pi */
object MPIPi {

  @throws[MPIException]
  def mpiop(mpargs: Array[String]): Unit = {

    BlazeUtils.setPmixEnv()
//    BlazeUtils.setRank(mpargs(0))

    val map = new util.HashMap[String, String]()
    map.put("PMIX_RANK", mpargs(0))
    NativeUtils.setEnv(map)

    MPI.Init(mpargs)
    val myrank = MPI.COMM_WORLD.getRank
    val size = MPI.COMM_WORLD.getSize

    System.out.println("Hello world from rank " + myrank + " size of " + size)
//    var points: Int = 0
//    if (myrank != 0) {
//      points = Int.MaxValue / size
//    } else {
//      points = Int.MaxValue - (Int.MaxValue / size) * (size - 1)
//    }
//    var cnt = 0
//    println(s"rank: ${myrank} ppn:${points}")
//    val random = new Random(myrank)
//    for (i <- 0 until points) {
//      val x = random.nextDouble() * 2 - 1
//      val y = random.nextDouble() * 2 - 1
//      if (x * x + y * y <= 1) cnt += 1
//      println(s"cnt is ${cnt}")
//    }
//    val msg = Array[Int](cnt)
//    val res = Array[Int]()
//    val tag = 50
//    println(s"rank ${myrank} send: ${msg(0)}")
//    MPI.COMM_WORLD.send(msg, 1, MPI.INT, 0, 50)
//    MPI.COMM_WORLD.reduce(msg, res, 1, MPI.INT, MPI.SUM, 0)
//
//    if (myrank == 0) {
//      println(s"rank ${myrank} recv: ${res(0)}")
//      print(res(0).toDouble / Int.MaxValue * 4)
//    }
    MPI.Finalize()

  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .set("spark.master", "spark://192.168.32.197:7077")
      .setJars(Array[String]("/home/xialb/opt/spark/blaze/target/spark-blaze_2.12-3.0.3-SNAPSHOT.jar"))
      .set("spark.executor.cores", "1")

    val blaze = BlazeSession
      .builder
      .appName("blazePi")
      .config(conf)
      .master("spark://192.168.32.197:7077")
      .getOrCreate()

    val slices = if (args.length > 0) args(0).toInt else 2
//    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
//    val count = blaze.mpiContext.parallelize(0 until 2, slices).map { i =>
//      val x = random * 2 - 1
//      val y = random * 2 - 1
//      if (x * x + y * y <= 1) 1 else 0
//    }.reduce(_ + _)
//
//    print(s"Pi is roughly ${4.0 * count / (n - 1)} \n")

    val mc = blaze.mpiContext
    mc.setLogLevel("INFO")
    mc.parallelize(0 until 2, slices).map(i => {
      val argv = Array(i.toString)
      mpiop(argv)
    }).collect()

    blaze.stop()
  }
}
