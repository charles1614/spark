
package org.apache.spark.examples.blaze

import scala.math.sqrt
import scala.sys.exit

import org.apache.log4j.Logger

import org.apache.spark.BlazeSession
import org.apache.spark.mllib.linalg.{DenseMatrix, Vectors}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}

import MPIOperators.mpiMultiply

object BlockMatrixMultiplyMPI {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    val size = if (args.size > 0) args(0).toInt else 900
    val partitions = if (args.size > 1) args(1).toInt else 9

    val blaze = BlazeSession.builder
      .appName("MPI BlockMatrixMultiply")
      .master("local[*]")
      .getOrCreate()

    val bc = blaze.blazeContext

    /* dense matrix size*size */
    val line = Vectors.dense(Array.fill[Double](size) {
      1
    })

    val mARows = bc.parallelize(1 to size, partitions).map(idx => IndexedRow(idx - 1, line))

    // row per block
    val rpb = size / sqrt(partitions).toInt
    val blockMat = new IndexedRowMatrix(mARows).toBlockMatrix(rpb, rpb)

    // block seq
    val blockSeq = blockMat.blocks.mpimap {
      case ((row, col), mat) =>
        //        println(s"mat is ${mat.toString}")
        mpiMultiply(mat.asInstanceOf[DenseMatrix], mat.asInstanceOf[DenseMatrix])
    }

    val t0 = System.nanoTime()
    val ret = blockSeq.count()
    val t1 = System.nanoTime()
    //    println(s"ret is ${ret/size/size}, elapse time is ${t1-t0}")
    println(s"ret is , elapse time is ${(t1 - t0)/1000000}ms")
//    val sum = blockSeq.map(n => n.values.sum).reduce(_ + _)
//    println(s"sum is ${sum}")

    //    val ret = LogElapsed.log("MLlib", size, partitions,
    //      blockSeq.map(block => block.toArray.sum).sum
    //    )

    //    log.info(s"ret head is ${ret / size / size} while expected is ${size}")

    //    val ret = LogElapsed.log("MPI", size, partitions,
    //      lambda.collect
    //    )

    //    log.info(s"ret head is ${ret / size / size} while expected is ${size}")

    bc.stop()
    exit(0)
  }
}
