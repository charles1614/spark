
package org.apache.spark.examples.blaze.cg

import org.apache.spark.BlazeSession
import org.apache.spark.examples.blaze.linalg.PrintHelper._
import org.apache.spark.examples.blaze.linalg.MatrixOp._
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.examples.blaze.linalg.{MatrixOp, NumPartitioner}

object SparkSGD {
  def main(args: Array[String]): Unit = {

    val blaze = BlazeSession
      .builder
      .appName("Spark SGD")
      //      .master("local[*]")
      .getOrCreate()

    val bc = blaze.blazeContext
    bc.setLogLevel("Error")

    val m: Int = if (args.length > 0) args(0).toInt else 2
    val n: Int = if (args.length > 1) args(1).toInt else 4
    val partitions: Int = if (args.length > 2) args(2).toInt else 2
    val maxIters: Int = if (args.length > 3) args(3).toInt else 1000

    val sample = new MatrixOp(bc)
    val blockMatrixA = sample.genSPD(m, n, partitions)
    val A = blockMatrixA.toVectorRDD(partitions)
    //    printVecRDD(A)

    val b = sample.genRandomVector(m, partitions)
    //    printVecRDD(b)

    /** gen Label */

    val bZipA = A.zip(b)

    //    printVecZipDns(bZipA)

//    bZipA.collect()
    val testRDD: RDD[LabeledPoint] = bZipA.map(iter => LabeledPoint(iter._2.toArray(0), iter._1))
    //    printLabelPoint(testRDD)

    /** linReg */
    val linReg = new LinearRegressionWithSGD(1, 10000, 0.0, 0.5)
    linReg.optimizer.setNumIterations(maxIters).setStepSize(0.1).setConvergenceTol(1E-9)

    val start = System.nanoTime
    val model = linReg.run(testRDD)
    val weights: Vector = model.weights
    val stop = System.nanoTime




    val vectors: Array[DenseVector] = weights.toArray.map(i => new DenseVector(Array(i)))
    vectors.foreach(i => i.values.foreach(i => print(i + " \n")))
    print(s"time is ${(stop-start)/1000000}")
    //
    //    val vectorsRDD: RDD[IndexedRow] = bc.parallelize(vectors)
    //      .zipWithIndex
    //      .map {
    //        case (vec, index) => new IndexedRow(index, vec)
    //      }
    //
    //    val xBlock = new IndexedRowMatrix(vectorsRDD, m, 1).toBlockMatrix()
    //
    //    val assertb: BlockMatrix = blockMatrixA.multiply(xBlock)
    //    val rows = assertb.toIndexedRowMatrix().toRowMatrix().rows
    //
    //    //    printVecRDD(rows)
    //    val rangex = rows.zipWithIndex().map(iter => (iter._2, iter._1))
    //    val x = rangex.partitionBy(new NumPartitioner(size)).map(iter => iter._2)
    //    val res = x.zip(b).map(i => i._1.asBreeze - i._2.asBreeze)
    //      .map(i => i.map(d => d * d).reduce(_ + _))
    //      .reduce(_ + _)
    //    val res = x.zip(b).map(i => i._1.asBreeze - i._2.asBreeze).map(i => i * i).reduce(_ + _)
    //    print(s"residual is ${res}")
  }
}
