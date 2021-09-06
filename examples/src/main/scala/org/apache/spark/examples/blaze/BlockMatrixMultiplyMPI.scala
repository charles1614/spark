
package org.apache.spark.examples.blaze

import scala.math.sqrt
import mpi.{CartComm, Intracomm, MPI}
import org.apache.log4j.Logger
import org.apache.spark.BlazeSession
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrix, Vectors}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, IndexedRow, IndexedRowMatrix}
import MPIOperators.mpiMultiply
import org.apache.spark.examples.MPIP1.mpiop
import org.apache.spark.rdd.RDD

import scala.sys.exit

object BlockMatrixMultiplyMPI {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    val size = if (args.size > 0) args(0).toInt else 2560
    val partitions = if (args.size > 1) args(1).toInt else 16

    val blaze = BlazeSession.builder
      .appName("MLlib BlockMatrixMultiply example")
      //      .master("local[4]")
      .getOrCreate()

    val bc = blaze.blazeContext

    //    val line = Vectors.dense(Array.fill[Double](size) {
    //      1
    //    })
    //
    //    val mARows = bc.parallelize(1 to size, partitions).map(idx => IndexedRow(idx - 1, line))
    //
    //    val blockMat = new IndexedRowMatrix(mARows).toBlockMatrix()
    //
    val block: Seq[((Int, Int), DenseMatrix)] = Seq(
      ((0, 0), new DenseMatrix(2, 2, Array(1.0, 0.0, 0.0, 2.0))),
      ((0, 1), new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 0.0))),
      ((1, 0), new DenseMatrix(2, 2, Array(3.0, 0.0, 1.0, 1.0))),
      ((1, 1), new DenseMatrix(2, 2, Array(1.0, 2.0, 0.0, 1.0))))

    val blocks: RDD[((Int, Int), Matrix)] = bc.parallelize(block, 4)
    //
    val matrix = new BlockMatrix(blocks, 2, 2)

    val lambda = matrix.blocks.mpimap {
      case ((row, col), mat) =>
        mpiMultiply(mat.asInstanceOf[DenseMatrix], mat.asInstanceOf[DenseMatrix])
    }.collect


    //    val ret = LogElapsed.log("MPI", size, partitions,
    //      lambda.collect
    //    )

    //    log.info(s"ret head is ${ret / size / size} while expected is ${size}")

    bc.stop()
    exit(0)
  }
}
