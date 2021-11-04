
package org.apache.spark.examples.blaze.linalg

import scala.language.implicitConversions

object ArrayHelper {
  implicit class ArrayOp(val oprand: Array[Double]) {

    def *(d: Double): Array[Double] = {
      oprand.map(iter => iter * d)
    }

    def dot(vec: Array[Double]): Double = {
      oprand.zip(vec).map(iter => iter._1 * iter._2).sum
    }


    def +(vec: Array[Double]): Array[Double] = {
      oprand.zip(vec).map(iter => iter._1 + iter._2)
    }

    def -(vec: Array[Double]): Array[Double] = {
      oprand.zip(vec).map(iter => iter._1 - iter._2)
    }

    implicit def arrayToArrayOp(array: Array[Double]): ArrayOp = {
      new ArrayOp(array)
    }
  }
}
