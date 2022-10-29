//
//package org.apache.spark.examples.blaze.cg
//
//import breeze.linalg
//import breeze.linalg.{DenseVector, Vector}
//import org.apache.spark.{BlazeContext, BlazeSession, mllib}
////import org.apache.spark.examples.blaze.linalg.MatrixOp
//import org.apache.spark.mllib.linalg.distributed.IndexedRow
//import org.apache.spark.rdd.RDD
//
//object SparkHdfCG {
//
//  /* TODO: hdf5 spd matrix must fit the size of parameter m NOW! */
//  def cg(blockMatrixA: RDD[IndexedRow],
//    b: DenseVector[Double],
//    m: Int,
//    maxIters: Int,
//    bc: BlazeContext): Unit = {
//    var r, rn = b
//    var p = r
//    var a, c: Double = 0
//    var x = Vector(Array.fill(m)(0.0))
//
//    var iter = 0
//    val part = blockMatrixA.getNumPartitions
//    print(s"part is ${part}")
//
//    val start = System.nanoTime
//    while (iter < maxIters) {
//
//      bc.broadcast(p)
//
//      iter = iter + 1
//
//      val pa = blockMatrixA.map { row => (row.index, row.vector.asBreeze.dot(p)) }
//      val arr: Array[Double] = pa.sortBy(iter => iter._1).collect().map(d => d._2)
//      val paVec: Vector[Double] = linalg.Vector(arr)
//
//      a = r.dot(r) / p.dot(paVec)
//      x = x - p * a
//      rn = r - paVec * a
//      c = r.dot(r) / rn.dot(rn)
//      p = rn + p * c
//    }
//  }
//
//  def main(args: Array[String]): Unit = {
//
//    val blaze = BlazeSession.builder
//      .appName("Spark HDF CG")
//         .master("local[*]")
//      .getOrCreate()
//
//    val bc = blaze.blazeContext
//    // bc.setLogLevel("ERROR")
//
//    val m: Int = if (args.length > 0) args(0).toInt else 100
//    //    val n: Int = if (args.length > 1) args(1).toInt else 4
//    val partitions: Int = if (args.length > 2) args(2).toInt else 10
//    val maxIters: Int = if (args.length > 3) args(3).toInt else 1000
//
//    val array = HdfRead.readDataSet("/home/xialb/tmp/test.h5", "data", m, m)
//
//    //    val sample = new MatrixOp(bc)
//    //    val blockMatrixA = sample.genSPD(m, n, partitions)
//    //    val rowMatrixA = blockMatrixA.toIndexedRowMatrix()
//    //    val rowRDD = rowMatrixA.rows.repartition(partitions)
//    val rowArray = Array.ofDim[IndexedRow](m)
//    for (i <- 0 until array(0).length) {
//      rowArray(i) = IndexedRow(i, new mllib.linalg.DenseVector(array(i)))
//    }
//    val rowRDD = bc.parallelize(rowArray, partitions)
//    rowRDD.cache()
//
//    val normal01 = breeze.stats.distributions.Gaussian(0, 1)
//    val b: DenseVector[Double] = DenseVector.rand(m, normal01)
//
//      cg(rowRDD, b, m, maxIters, bc)
//  }
//}
