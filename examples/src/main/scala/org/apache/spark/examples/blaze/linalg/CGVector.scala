
package org.apache.spark.examples.blaze.linalg

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * A Vector object is stored as an RDD of (key,value) pairs where
 * key is the row id and value is the corresponding vector element.
 * The Vector is represented as a dense structure (also zero elements are stored)
 *
 * @param rdd : RDD[(id:Int, element:Double)]
 */
class CGVector(var rdd: RDD[(Int, Double)] = null) {

  def init(spark: SparkContext, array: Array[Double]) {
    rdd = spark.parallelize(for (i <- array.indices) yield (i, array(i)))
  }

  def size(): Int = {
    rdd.count().toInt
  }

  def magnitude(): Double = {
    math.sqrt(this dot this)
  }

  def elementWise_byScalar(op: (Double, Double) => Double, k: Double): CGVector = {
    new CGVector(rdd.map { case (i, x) => (i, op(x, k)) })
  }

  def +(k: Double): CGVector = elementWise_byScalar(op = _ + _, k = k)

  def -(k: Double): CGVector = elementWise_byScalar(op = _ - _, k = k)

  def *(k: Double): CGVector = elementWise_byScalar(op = _ * _, k = k)

  def /(k: Double): CGVector = elementWise_byScalar(op = _ / _, k = k)

  def elementWise_byVector(op: (Double, Double) => Double, that: CGVector): CGVector = {
    new CGVector(this.rdd.join(that.rdd).map { case (i, (x, y)) => (i, op(x, y)) })
  }

  def +(that: CGVector): CGVector = elementWise_byVector(op = _ + _, that = that)

  def -(that: CGVector): CGVector = elementWise_byVector(op = _ - _, that = that)

  def *(that: CGVector): CGVector = elementWise_byVector(op = _ * _, that = that)

  def /(that: CGVector): CGVector = elementWise_byVector(op = _ / _, that = that)

  def dot(that: CGVector): Double = {
    this.rdd.join(that.rdd)
      .map { case (_, (x, y)) => x * y }
      .reduce(_ + _)
  }

  def show() {
    val s: Int = size()
    val array = Array.ofDim[Double](n1 = s)
    for ((i, x) <- rdd.collect) array(i) = x
    print("[")
    for (i <- array.indices)
      if (i == size - 1) print(array(i))
      else print(array(i) + ", ")
    println("]")
  }

}
