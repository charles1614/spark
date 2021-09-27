
package org.apache.spark.examples.blaze.cg.test

import org.apache.spark.BlazeSession
import org.apache.spark.examples.blaze.cg.MatrixOpMPI
import org.apache.spark.examples.blaze.linalg.VectorSpace.DVector
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.random.{RandomRDDs, StandardNormalGenerator}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

class zMatrixOpTest {


}

object MatrixOpTest {
  def main(args: Array[String]): Unit = {
    val blaze = BlazeSession
      .builder
      .appName("blazePi")
      .master("local[*]")
      .getOrCreate()

    val bc = blaze.blazeContext

    val size = 5
    val m = 5
    val n = 1
    val x = RandomRDDs.normalRDD(bc, m, size, 1).glom.map(new DenseVector(_))
    val A: RDD[Vector] = RandomRDDs.randomVectorRDD(bc, new StandardNormalGenerator, m, n, size, 1)
    val value = A.zip(x)
//    val b = MatrixOpMPI.MatMultVec(A, x, bc)
    //    val unnormalizedA = RandomRDDs.normalVectorRDD(bc, m, n, 3, 1)
  }

}
