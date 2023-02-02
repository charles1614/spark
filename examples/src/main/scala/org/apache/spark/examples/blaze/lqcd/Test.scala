
package org.apache.spark.examples.blaze.lqcd

import org.apache.spark.BlazeSession

object Test {

  def main(args: Array[String]): Unit = {
    val blaze = BlazeSession
      .builder
      .appName("SleepTest")
      //            .master("spark://besh01:7077")
      //      .config("spark.executor.memory", "2g")
      //      .config("spark.executor.cores", "1")
      //      .config("spark.cores.max", "8")
      //      .master("local[*]")
      .getOrCreate()

    val bc = blaze.blazeContext
    val exe = "sleep"

    //    var start, end: Long = 0
    val size = if (args.size > 0) args(0).toInt else 8

    val list = (1 to size).map(_ => s"10m")
    val data = bc.parallelize(list, size)

    // scalastyle:off println
    //    val res = data.collect().foreach(println)

    //        val res = data.mpipipe(Seq(exe))
    val res = data.mpipipe(Seq(exe))
    res.collect().foreach(println)

    // scalastyle:on println
    blaze.stop()
    System.exit(0)
  }
}
