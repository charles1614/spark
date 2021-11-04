
// scalastyle:off println
package org.apache.spark.examples

import scala.util.Random

import mpi.{ MPI, MPIException}

import org.apache.spark.BlazeSession
import org.apache.spark.SparkConf


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
//      .set("spark.master", "spark://192.168.32.197:7077")
//      .setJars(Array[String]("/home/xialb/opt/spark/blaze/target/spark-blaze_2.12-3.0.3-SNAPSHOT.jar"))
      .set("spark.executor.cores", "1")
//      .set("spark.dynamicAllocation.maxExecutors", "7")
//      .set("spark.task.cpus","1")


    val blaze = BlazeSession
      .builder
      .config(conf)
      .appName("blazePi")
      .master("local[2]")
      .getOrCreate()

//    val slices = if (args.length > 0) args(0).toInt else 7

    val bc = blaze.blazeContext

    bc.setLogLevel("INFO")

    val pi = bc.parallelize(0 until 2, 2).map(i => {
      val argv = Array(i.toString)
      mpiop(argv)
    }).collect()

    val bstop_p = System.currentTimeMillis()

    pi.map(i => println(s"pi array ${i}"))


    blaze.stop()
    println("MPIPI has exited")
    System.exit(0)
  }
}
