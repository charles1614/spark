
// scalastyle:off println
package org.apache.spark.examples

import java.util

import scala.math.random
import scala.util.Random

import mpi.{Comm, MPI, MPIException}

import org.apache.spark.BlazeSession
import org.apache.spark.SparkConf
import org.apache.spark.blaze.BlazeUtils
import org.apache.spark.blaze.deploy.mpi.NativeUtils
import org.apache.spark.blaze.ompi.MPIConf


/** Computes an approximation to pi */
object MPIPi {

  @throws[MPIException]
  def mpiop(mpargs: Array[String]): Double = {

    MPI.Init(mpargs)

    val myrank = MPI.COMM_WORLD.getRank
    val size = MPI.COMM_WORLD.getSize

    val points = 100000;
    var ppn: Int = 0
    if (myrank != 0) {
      ppn = points / size
    } else {
      ppn = points - (points / size) * (size - 1)
    }
    var cnt = 0
    println(s"rank: ${myrank} ppn:${ppn}")
    val random = new Random(myrank)
    for (i <- 0 until ppn) {
      val x = random.nextDouble() * 2 - 1
      val y = random.nextDouble() * 2 - 1
      if (x * x + y * y <= 1) cnt += 1
    }
    val send = Array[Int](cnt)
    var recv = Array[Int](cnt)
    val tag = 50
    MPI.COMM_WORLD.send(send, 1, MPI.INT, 0, 50)
    MPI.COMM_WORLD.reduce(send, recv, 1, MPI.INT, MPI.SUM, 0)

    var res: Double = 0
    if (myrank == 0) {
      res = (recv(0).toDouble / points * 4)
    }
    MPI.Finalize()
    res
  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .set("spark.master", "spark://192.168.32.197:7077")
      .setJars(Array[String]("/home/xialb/opt/spark/blaze/target/spark-blaze_2.12-3.0.3-SNAPSHOT.jar"))
      .set("spark.executor.cores", "1")

    val bstart = System.currentTimeMillis()

    val blaze = BlazeSession
      .builder
      .appName("blazePi")
      .config(conf)
      .getOrCreate()

//    val slices = if (args.length > 0) args(0).toInt else 7

    val bc = blaze.blazeContext

    val binit = System.currentTimeMillis()

    bc.setLogLevel("INFO")

    val bstart_p = System.currentTimeMillis()

    val pi = bc.parallelize(0 until 7, 7).map(i => {
      val argv = Array(i.toString)
      mpiop(argv)
    }).collect()

    val bstop_p = System.currentTimeMillis()

    pi.map(i => println(s"pi array ${i}"))

    BlazeUtils.getElapseTime(bstart, binit, "Blaze Init")
    BlazeUtils.getElapseTime(bstart_p, bstop_p, "Blaze Compute")

    blaze.stop()
    println("MPIPI has exited")
    System.exit(0)
  }
}
