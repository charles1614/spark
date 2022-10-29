
package org.apache.spark.examples.blaze.cg

import breeze.linalg
import breeze.linalg.{DenseVector, Vector}
import org.apache.spark.{BlazeContext, BlazeSession, mllib}

//import org.apache.spark.examples.blaze.linalg.MatrixOp
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.rdd.RDD

object SparkMCG {

  def cg(blockMatrixA: RDD[IndexedRow],
    b: DenseVector[Double],
    m: Int,
    maxIters: Int,
    bc: BlazeContext): Vector[Double] = {
    var r, rn = b
    var p = r
    var a, c: Double = 0
    var x = Vector(Array.fill(m)(0.0))

    var iter = 0
    val part = blockMatrixA.getNumPartitions
    //    print(s"part is ${part}")

    val start = System.nanoTime
    while (iter < maxIters) {

      bc.broadcast(p)

      iter = iter + 1

      val pa = blockMatrixA.map { row => (row.index, row.vector.asBreeze.dot(p)) }
      val arr: Array[Double] = pa.sortBy(iter => iter._1).collect().map(d => d._2)
      val paVec: Vector[Double] = linalg.Vector(arr)

      a = r.dot(r) / p.dot(paVec)
      x = x - p * a
      rn = r - paVec * a
      c = r.dot(r) / rn.dot(rn)
      p = rn + p * c
    }
    x
  }

  def main(args: Array[String]): Unit = {

    val blaze = BlazeSession.builder
      .appName("Spark MCG")
//            .master("local[*]")
      .getOrCreate()

    val bc = blaze.blazeContext
    bc.setLogLevel("ERROR")

    val m: Int = if (args.length > 0) args(0).toInt else 40000
    //    val n: Int = if (args.length > 1) args(1).toInt else 4
    val partitions: Int = if (args.length > 1) args(1).toInt else 20
    val maxIters: Int = if (args.length > 2) args(2).toInt else 10

    val local_row = m / partitions

    //    var array = Array.ofDim[Double](partitions, local_row, m)
    val array: RDD[Array[Array[Double]]] = bc.parallelize(0 until partitions, partitions).map { i =>
      val start: Array[Long] = Array(i.toLong * local_row, 0)
      val array: Array[Array[Double]] =
        HdfRead.readDataSet("/home/xialb/tmp/matrix_50000.h5", "data", local_row, m, start)
      array
    }

    val rowRDD: RDD[IndexedRow] = array.flatMap { arr =>
      val rowArray = Array.ofDim[IndexedRow](local_row)
      for (i <- 0 until local_row) {
        rowArray(i) = IndexedRow(i, new mllib.linalg.DenseVector(arr(i)))
      }
      rowArray
    }

    //    val row: RDD[IndexedRow] = rowRDD.map{ arr =>
    //      arr.toList.map(row => {row.index, _})
    //    }


    //        val rowArray = Array.ofDim[IndexedRow](m)

    //        val rowRDD = bc.parallelize(rowArray, partitions)
    rowRDD.cache()
    print(rowRDD.count())


    //    println(c)
    //    print(array(0)(0)(0))
    //    print(array(50000)(0)(0))

    //    val array = HdfRead.readDataSet("/home/xialb/tmp/matrix_50000.h5", "data", m, m)

    //    val sample = new MatrixOp(bc)
    //    val blockMatrixA = sample.genSPD(m, n, partitions)
    //    val rowMatrixA = blockMatrixA.toIndexedRowMatrix()
    //    val rowRDD = rowMatrixA.rows.repartition(partitions)
    //    val rowArray = Array.ofDim[IndexedRow](m)
    //    for (i <- 0 until array(0).length) {
    //      rowArray(i) = IndexedRow(i, new mllib.linalg.DenseVector(array(i)))
    //    }
    //    val rowRDD = bc.parallelize(rowArray, partitions)
    //    rowRDD.cache()
    //
    val normal01 = breeze.stats.distributions.Gaussian(0, 1)
    val b: DenseVector[Double] = DenseVector.rand(m, normal01)
    //    //
    val x = cg(rowRDD, b, m, maxIters, bc)
    println("finished: " + x.length)
  }
}
