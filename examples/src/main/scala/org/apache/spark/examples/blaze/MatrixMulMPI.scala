
package org.apache.spark.examples.blaze

import org.apache.spark.examples.blaze.MPIOperators.mpiMultiply
import org.apache.spark.mllib.linalg.DenseMatrix

object MatrixMulMPI {

  def main(args: Array[String]): Unit = {
    val size = if (args.size > 0) args(0).toInt else 900
    val partitions = if (args.size > 1) args(1).toInt else 9

    val matrix = new DenseMatrix(size, size, Array.fill(size * size)(1))
//    val blocks: Seq[((Int, Int), DenseMatrix)] = Seq(
//      ((0, 0), new DenseMatrix(2, 2, Array(1.0, 0.0, 0.0, 2.0))),
//      ((0, 1), new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 0.0))),
//      ((1, 0), new DenseMatrix(2, 2, Array(3.0, 0.0, 1.0, 1.0))),
//      ((1, 1), new DenseMatrix(2, 2, Array(1.0, 1.0, 1.0, 1.0))))
//    val ret = mpiMultiply(blocks(3)._2, blocks(3)._2)
    mpiMultiply(matrix, matrix)
  }
}
