
package org.apache.spark.examples.blaze.lqcd

import org.apache.spark.BlazeSession

object HMC {

  def main(args: Array[String]): Unit = {
    val blaze = BlazeSession
      .builder
      .appName("MPIPipe")
      .master("spark://lenovo:7077")
      .config("spark.executor.memory", "2g")
      .config("spark.executor.cores", "1")
      .config("spark.cores.max", "8")
      //      .master("local[*]")
      .getOrCreate()

    val bc = blaze.blazeContext
    val exe = System.getenv("HOME") +
      "/git/lqcdworkflow/code/01.chroma_build/install/chroma-double/bin/hmc"

    //    var start, end: Long = 0
    val size = if (args.size > 0) args(0).toInt else 8

    val base = "/home/xialb/git/lqcdworkflow/tests/01.hmc"
    val input = base + "/input/hmc.prec_wilson.ini.xml"
    val output = base + "/output/hmc.prec_wilson.out.xml"
    val hmclog = base + "/output/hmc.prec_wilson.log.xml"

    val list = (1 to size).map(_ => s"-geom 1 1 2 4 -i ${input} -o ${output} -hmclog ${hmclog}")
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
