
package org.apache.spark.examples.blaze.cg

import org.apache.spark.{BlazeContext, BlazeSession}
import org.apache.spark.examples.blaze.linalg.{CGMatrix, CGVector}

object SparkCG {

  var bc: BlazeContext = _

  def main(args: Array[String]): Unit = {
    val blaze = BlazeSession
      .builder
      .appName("SparkCG")
      //      .master("local[*]")
      .getOrCreate()

    bc = blaze.blazeContext
    bc.setLogLevel("Error")

    val size: Int = if (args.length > 0) args(0).toInt else 5
    val maxIters: Int = if (args.length > 1) args(1).toInt else 1000
    val resi: Double = if (args.length > 2) args(2).toDouble else 1E-6

    val A: CGMatrix = CGMatrix.get_randomMatrixSPD(bc, size = size) * 6 - 3
    val b: CGVector = CGMatrix.get_randomVector(bc, size = size) * 4 - 2
    val x: CGVector = conjugate_gradient(A = A, b = b, tol = resi, maxiters = maxIters)
    val check: CGVector = (A dot x) - b
    println(check.magnitude())
    bc.stop()
  }


  def conjugate_gradient(A: CGMatrix, b: CGVector,
                         tol: Double = 1E-6, maxiters: Int = 1000): CGVector = {
    //    A.show()
    //    b.show()
    var x: CGVector = CGMatrix.get_randomVector(bc, size = b.size())
    var r: CGVector = b - (A dot x)
    var p: CGVector = r
    var iter = 0

    val start = System.nanoTime

    while (iter < maxiters) {
      iter = iter + 1
      println(iter)
      val Ap: CGVector = A dot p
      val alpha_den: Double = p dot Ap
      val r_squared: Double = r dot r
      val alpha: Double = r_squared / alpha_den
      x = x + p * alpha
      r = r - Ap * alpha
      val beta_num: Double = r dot r
      if (beta_num < tol) {
        iter = maxiters
      }
      val beta: Double = beta_num / r_squared
      p = r + p * beta
    }

    val stop = System.nanoTime
    print(s"time: ${(stop - start)/1E6}\n")

    //    x.show()
    x
  }

}
