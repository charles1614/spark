
package org.apache.spark.examples.blaze

import org.apache.log4j.Logger
import org.apache.spark.BlazeSession
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}

object BlockMatrixMultiply {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    val size = if (args.size > 0) args(0).toInt else 2560
    val partitions = if (args.size > 1) args(1).toInt else 16

    val blaze = BlazeSession.builder
      .appName("MLlib BlockMatrixMultiply")
      .getOrCreate()
    val bc = blaze.blazeContext

    val line = Vectors.dense(Array.fill[Double](size) {
      1
    })

    val mARows = bc.parallelize(1 to size, partitions).map(idx => IndexedRow(idx - 1, line))

    val blockMat = new IndexedRowMatrix(mARows).toBlockMatrix()

    //    val watch = new StopWatch()
    val start = System.nanoTime
    //    watch.start()
    //    val ret = LogElapsed.log("MLlib", size, partitions,
    //      blockMat.multiply(blockMat).blocks.map(_._2.toArray.sum).sum
    //    )
    val mat = blockMat.multiply(blockMat)
    val stop = System.nanoTime
      mat.blocks.count()


    //    mat.blocks.map(_._2.toArray.sum).sum

    //    println(s"ret head is ${ret / size / size} while expected is ${size}")
    println(s"elapse time is ${(stop - start) / 1E6}")

    bc.stop()
  }
}
