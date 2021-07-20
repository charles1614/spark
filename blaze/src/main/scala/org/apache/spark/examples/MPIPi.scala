
// scalastyle:off println
package org.apache.spark.examples

import java.util

import scala.math.random
import scala.util.Random

import mpi.{Comm, MPI, MPIException}

import org.apache.spark.SparkConf
import org.apache.spark.blaze.{BlazeSession, BlazeUtils}
import org.apache.spark.blaze.deploy.mpi.NativeUtils
import org.apache.spark.blaze.ompi.MPIConf



/** Computes an approximation to pi */
object MPIPi {

  @throws[MPIException]
  def mpiop(mpargs: Array[String]): Unit = {

    BlazeUtils.setPmixEnv()
    BlazeUtils.setRank(mpargs(0))

    MPI.Init(mpargs)
    val myrank = MPI.COMM_WORLD.getRank
    val size = MPI.COMM_WORLD.getSize

    val points = 10000;
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

    if (myrank == 0) {
      print(recv(0).toDouble / points * 4)
    }
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
      .getOrCreate()

    val slices = if (args.length > 0) args(0).toInt else 2

    val mc = blaze.mpiContext
    mc.setLogLevel("INFO")
    mc.parallelize(0 until 2, slices).map(i => {
      val argv = Array(i.toString)
      mpiop(argv)
    }).collect()

    blaze.stop()
  }
}
