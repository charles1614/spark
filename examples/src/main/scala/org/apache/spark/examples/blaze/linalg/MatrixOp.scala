
package org.apache.spark.examples.blaze.linalg

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.random.{RandomRDDs, StandardNormalGenerator}
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions

class MatrixOp(val bc: SparkContext) {

  def genRandomVector(m: Int, size: Int): RDD[Vector] = {
    val originalb: RDD[DenseVector] = RandomRDDs
      .randomVectorRDD(bc, new StandardNormalGenerator, m, 1, size, 1)
      .asInstanceOf[RDD[DenseVector]]

    val rangeb: RDD[(Long, Vector)] = originalb.zipWithIndex().map(iter => (iter._2, iter._1))
    val b = rangeb.partitionBy(new NumPartitioner(size)).map(iter => iter._2)
    b
  }

  def genBlockMatrixVector(m: Int, size: Int): BlockMatrix = {

    val originalA: RDD[Vector] = RandomRDDs
      .randomVectorRDD(bc, new StandardNormalGenerator, m, 1, size, 1)
    val indexA = originalA.zipWithIndex()
    val row: RDD[IndexedRow] = indexA.map(iter => new IndexedRow(iter._2, iter._1))

    new IndexedRowMatrix(row, m, 1).toBlockMatrix()
  }

  /** gen m*m SPD matrix and partitioned by NumPartitioner */
  def genSPD(m: Int, n: Int, size: Int): BlockMatrix = {

    val originalA: RDD[Vector] = RandomRDDs
      .randomVectorRDD(bc, new StandardNormalGenerator, m, n, size, 1)
    val indexA = originalA.zipWithIndex()
    val row: RDD[IndexedRow] = indexA.map(iter => new IndexedRow(iter._2, iter._1))

    // A * A'
    val partitions = math.sqrt(size).toInt + 1
    val ir = new IndexedRowMatrix(row, m, n)

    val BlockA = new IndexedRowMatrix(row, m, n).toBlockMatrix(partitions, partitions)
    val matrixSPD: BlockMatrix = BlockA.multiply(BlockA.transpose)
    matrixSPD
  }

}

object MatrixOp {
  implicit class BlockMatrixPlugin(block: BlockMatrix) {
    def toVectorRDD(size: Int): RDD[Vector] = {
      val A: RDD[IndexedRow] = block.toIndexedRowMatrix().rows
      val sortC = A.sortBy(ord => ord.index, numPartitions = size)
      val rangeC: RDD[(Long, Vector)] = A.map(iter => (iter.index, iter.vector))
      val C = rangeC.partitionBy(new NumPartitioner(size)).map(iter => iter._2)
      C
    }

    implicit def blockMatrixToVector(block: BlockMatrix): BlockMatrixPlugin = {
      new BlockMatrixPlugin(block)
    }
  }
}


