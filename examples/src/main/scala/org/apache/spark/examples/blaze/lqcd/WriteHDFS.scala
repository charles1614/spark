
package org.apache.spark.examples.blaze.lqcd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}

import java.net.URI

class TmpFileFilter extends PathFilter {
  override def accept(path : Path): Boolean = path.getName.endsWith(".lime")
}

object WriteHDFS {

  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession
      .builder()
      .appName("WriteHDFS")
      .master("spark://lenovo:7077")
      .config("spark.executor.memory", "4g")
      .config("spark.executor.cores", "1")
      .config("spark.cores.max", "4")
      .getOrCreate()

    val sc = spark.sparkContext
    val path = new Path("/lqcd")
    val conf = new Configuration()
//    conf.set ("fs.defaultFS", "hdfs://helion01:8020")
    val fs = FileSystem.get(new URI("hdfs://helion01:8020"), conf)
    fs.mkdirs(path)
//    fs.copyFromLocalFile(new Path("/home/xialb/git/lqcdworkflow/tests/01.hmc/cfgs"), path)
    val limes = fs.globStatus(new Path("/lqcd/cfgs/*"), new TmpFileFilter())
    for (lime <- limes) {
      val path = lime.getPath
      fs.copyToLocalFile(path, new Path("/dev/shm/cfgs/" + path.getName))
    }
    sc.makeRDD(limes.map("/dev/shm/cfgs/" + _.getPath.getName)).collect().foreach(println)
//    fs.copyToLocalFile(new Path("/lqcd/cfgs/*.lime"), new Path("/dev/shm"))


    spark.stop()
  }
}
