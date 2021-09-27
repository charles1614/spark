
package org.apache.spark.examples.blaze.linalg

import scala.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * A Matrix object is stored as an RDD of (key1,(key2,value)) pairs where
 * key1 is the column id, key2 is the row id, and value is the corresponding
 * matrix element.
 * The Matrix is represented as a dense structure (also zero elements are stored)
 *
 * @param rdd : RDD[(col:Int, (row:Int, element:Double))]
 */
class CGMatrix(var rdd: RDD[(Int, (Int, Double))] = null) {

  def init(spark: SparkContext, array: Array[Array[Double]]) {
    rdd = spark.parallelize(for {
      i <- array.indices
      j <- array(i).indices
    } yield (j, (i, array(i)(j))))
  }

  def size(): Int = {
    math.sqrt(rdd.count).toInt
  }

  def transpose(): CGMatrix = {
    new CGMatrix(rdd.map { case (j, (i, x)) => (i, (j, x)) })
  }

  def elementWise_byScalar(op: (Double, Double) => Double, k: Double): CGMatrix = {
    new CGMatrix(rdd.map { case (j, (i, x)) => (j, (i, op(x, k))) })
  }

  def -(k: Double): CGMatrix = elementWise_byScalar(op = _ - _, k = k)

  def *(k: Double): CGMatrix = elementWise_byScalar(op = _ * _, k = k)

  def +(k: Double): CGMatrix = elementWise_byScalar(op = _ + _, k = k)

  def /(k: Double): CGMatrix = elementWise_byScalar(op = _ / _, k = k)

  def dot(vec: CGVector): CGVector = {
    new CGVector(rdd.groupByKey()
      .join(vec.rdd)
      .flatMap { case (_, v) =>
        v._1.map(mv => (mv._1, mv._2 * v._2))
      }
      .reduceByKey(_ + _))
  }

  def dot(mat: CGMatrix): CGMatrix = {
    val rddT = mat.rdd.map { case (k, (j, x)) => (j, (k, x)) }
    new CGMatrix(rdd.join(rddT)
      .map { case (_, ((i, x), (k, y))) => ((i, k), x * y) }
      .reduceByKey(_ + _)
      .map { case ((i, k), sum) => (k, (i, sum)) })
  }

  def show() {
    val s: Int = size()
    val array = Array.ofDim[Double](n1 = s, n2 = s)
    for ((j, (i, x)) <- rdd.collect) array(i)(j) = x
    print('[')
    for (i <- array.indices) {
      print("[")
      for (j <- array(i).indices) {
        if (j == size - 1) print(array(i)(j))
        else print(array(i)(j) + ", ")
      }
      if (i == size - 1) println("]]")
      else println("]")
    }
  }
}

object CGMatrix {
  def get_randomMatrixSPD(bc: SparkContext, size: Int): CGMatrix = {
    val array: Array[Array[Double]] =
      (for (_ <- 0 until size) yield
        (for (_ <- 0 until size) yield
          Random.nextDouble).toArray).toArray
    val matrix = new CGMatrix
    matrix.init(bc, array = array)
    matrix dot matrix.transpose()
  }

  def get_randomVector(bc: SparkContext, size: Int): CGVector = {
    val array: Array[Double] =
      (for (_ <- 0 until size)
        yield Random.nextDouble).toArray
    val vector = new CGVector
    vector.init(spark = bc, array = array)
    vector
  }
}
