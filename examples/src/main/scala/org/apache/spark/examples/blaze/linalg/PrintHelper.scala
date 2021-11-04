
package org.apache.spark.examples.blaze.linalg

import scala.language.implicitConversions

import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object PrintHelper {
  def printVecRDD(rdd: RDD[Vector]): Unit = {
    rdd.zipWithIndex.sortBy(_._2).foreach {
      case (vec, index) =>
        print(s"index: ${index} vector: ${vec}\n")
    }
  }

  def printVecZipDns(rdd: RDD[(Vector, Vector)]): Unit = {
    rdd.foreach {
      case (vec: Vector, dense: Vector) =>
        print(s"${dense} <- ${vec}\n")
    }
  }

  def printLabelPoint(rdd: RDD[LabeledPoint]): Unit = {
    rdd.foreach(iter =>
      print(s"LabelPoint: ${iter.label} <- ${iter.features}\n")
      )
  }

  implicit def DnsToVec(rdd: RDD[DenseVector]): RDD[Vector] = {
    rdd.asInstanceOf[RDD[Vector]]
  }
}
