
package org.apache.spark.examples.blaze

import mpi.{MPI, MPIException}
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.BlazeSession

import scala.util.Random

/** Computes an approximation to pi */
object MPIP1 {

  @throws[MPIException]
  def mpiop(mpargs: Array[String]): Double = {

    val stopWatch = new StopWatch()
    stopWatch.start

    MPI.Init(mpargs)

    val myrank = MPI.COMM_WORLD.getRank
    val size = MPI.COMM_WORLD.getSize

    val points = Int.MaxValue;
    var ppn: Int = 0
    if (myrank != 0) {
      ppn = points / size
    } else {
      ppn = points - (points / size) * (size - 1)
    }
    var cnt = 0
    //    println(s"rank: ${myrank} ppn:${ppn}")
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

    stopWatch.stop()
    println(s"elapse time is ${stopWatch.getTime}")
    MPI.Finalize()

    res
  }


  def main(args: Array[String]): Unit = {


    val blaze = BlazeSession
      .builder
      .appName("blazePi")
//      .master("local[*]")
      .getOrCreate()

    val bc = blaze.blazeContext

    var start, end: Long = 0


    val size = if (args.size > 0) args(0).toInt else 4
    val data = bc.parallelize(0 until size, size)

    val res = data.mpimap { i =>
      val argv = Array(i.toString)
      start = System.nanoTime()
      val r = mpiop(argv)
      end = System.nanoTime()
      r
    }.reduce(_ + _)


    println(s"spark elapse time is ${end - start}ms")
    println(s"pi is ${res}")

    blaze.stop()
    println("MPIPI has exited")
    System.exit(0)
  }
}
