
package org.apache.spark.examples.blaze.cg

import org.apache.spark.BlazeSession
import org.apache.spark.examples.blaze.linalg.MatrixOp
import org.apache.spark.examples.blaze.linalg.MatrixOp.BlockMatrixPlugin
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD

object MPICG {
  def main(args: Array[String]): Unit = {

    val blaze = BlazeSession.builder
      .appName("MPI BlockMatrixMultiply")
                  .master("local")
      .getOrCreate()

    val bc = blaze.blazeContext
    bc.setLogLevel("ERROR")

    val m: Int = if (args.length > 0) args(0).toInt else 2
    val n: Int = if (args.length > 1) args(1).toInt else 4
    val partitions: Int = if (args.length > 2) args(2).toInt else 2
    val maxIters: Int = if (args.length > 3) args(3).toInt else 1000

    val sample = new MatrixOp(bc)
    val blockMatrixA = sample.genSPD(m, n, partitions)
    val A: RDD[Vector] = blockMatrixA.toVectorRDD(partitions)
    //    printVecRDD(A)

    val b: RDD[Vector] = sample.genRandomVector(m, partitions)
    //    printVecRDD(b)
    val bd = b.asInstanceOf[RDD[DenseVector]]

    val data: RDD[(Vector, DenseVector)] = A.zip(bd)
    //    printVecZipDns(A.zip(bd).asInstanceOf[RDD[(Vector, Vector)]])

    // print(data.getNumPartitions)

    val ret = data.mpimapPartitions {
      iter =>
        ConjugateGradients.cg(iter, m, maxIters)
    }

    blaze.time {
      ret.count()
    }


//    for (elem <- ret.slice(0, ret.length / 2)) {
//      print(elem + "\n")
//    }
  }
}
