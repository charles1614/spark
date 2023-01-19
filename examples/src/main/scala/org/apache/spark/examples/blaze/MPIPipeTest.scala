
package org.apache.spark.examples.blaze

//import scala.util.Random

//import mpi.{MPI, MPIException}
//import org.apache.commons.lang3.time.StopWatch

import org.apache.spark.BlazeSession
/** Computes an approximation to pi */
object MPIPipeTest {


  def main(args: Array[String]): Unit = {


    val blaze = BlazeSession
      .builder
      .appName("MPIPipe")
//      .master("local[*]")
      .getOrCreate()

    val bc = blaze.blazeContext

    var start, end: Long = 0

    val size = if (args.size > 0) args(0).toInt else 4
    val data = bc.parallelize(0 until size, size)

    val res = data.mpipipe(Seq(System.getenv("HOME") + "/git/ompi/examples/hello_c"))
//    val res = data.mpipipe(Seq("hostname"))
    // scalastyle:off println
    res.collect().foreach(println)

    blaze.stop()
    println("MPIPipe has exited")
    // scalastyle:on println
    System.exit(0)
  }
}
