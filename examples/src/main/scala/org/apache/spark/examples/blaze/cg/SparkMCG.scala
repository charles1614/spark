
package org.apache.spark.examples.blaze.cg

import breeze.linalg
import breeze.linalg.{DenseVector, Vector}

import org.apache.spark.BlazeSession
import org.apache.spark.examples.blaze.linalg.{MatrixOp}

object SparkMCG {
  def main(args: Array[String]): Unit = {

    val blaze = BlazeSession.builder
      .appName("Spark MCG")
//      .master("local[*]")
      .getOrCreate()

    val bc = blaze.blazeContext
    bc.setLogLevel("ERROR")

    val m: Int = if (args.length > 0) args(0).toInt else 100
    val n: Int = if (args.length > 1) args(1).toInt else 4
    val partitions: Int = if (args.length > 2) args(2).toInt else 10
    val maxIters: Int = if (args.length > 3) args(3).toInt else 1000

    val sample = new MatrixOp(bc)
    val blockMatrixA = sample.genSPD(m, n, partitions)

    val normal01 = breeze.stats.distributions.Gaussian(0, 1)
    val b: DenseVector[Double] = DenseVector.rand(m, normal01)

    var r, rn = b
    var p = r
    var a, c: Double = 0
    var x = Vector(Array.fill(m)(0.0))

    var iter = 0
    val rowMatrixA = blockMatrixA.toIndexedRowMatrix()
    rowMatrixA.rows.cache()

    val start = System.nanoTime
    while (iter < maxIters) {

      bc.broadcast(p)

      iter = iter + 1

      val pa = rowMatrixA.rows.map { row => (row.index, row.vector.asBreeze.dot(p)) }
      val arr: Array[Double] = pa.sortBy(iter => iter._1).collect().map(d => d._2)
      val paVec: Vector[Double] = linalg.Vector(arr)

      a = r.dot(r) / p.dot(paVec)
      x = x - p * a
      rn = r - paVec * a
      c = r.dot(r) / rn.dot(rn)
      p = rn + p * c
    }

    val stop = System.nanoTime


    print(s"elapse time is ${
      (stop - start) / 1E6
    }ms")
  }
}
