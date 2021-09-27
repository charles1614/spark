
package org.apache.spark.examples.blaze.linalg

import org.apache.spark.mllib.linalg.{DenseVector, Vector}

class VectorOp {

}

object VectorOp {
  implicit class VectorPlugin(vec1: Vector) {
    def - (vec2: Vector): Double = {
      vec1.asBreeze.-(vec2.asBreeze).map(x => x * x).reduce(_ + _)
    }
  }


  implicit def vectorToVectorOp(vector: Vector): VectorPlugin = {
    new VectorPlugin(vector)
  }
}
