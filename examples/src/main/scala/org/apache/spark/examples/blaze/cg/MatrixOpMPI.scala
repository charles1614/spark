
package org.apache.spark.examples.blaze.cg

import mpi.MPI

import org.apache.spark.mllib.linalg.{DenseVector, Vector}

class MatrixOpMPI {

  def MatMultVec(mat: Iterator[DenseVector], vec: DenseVector): DenseVector = {
    val retv = mat.map(matv => matv.dot(vec)).toArray
    new DenseVector(retv)
  }


  // vecA and vecB are all part(local) of DVector, allReduce to sum
  def VecMultVec(vecA: DenseVector, vecB: DenseVector): Double = {
    val localDot = vecA.dot(vecB)
    var ret: Double = 0
    MPI.COMM_WORLD.allReduce(localDot, ret, 1, MPI.DOUBLE, MPI.SUM)
    ret
  }

}

object MatrixOpMPI {
//  def MatMultVec(mat: RDD[Vector], vec: RDD[DenseVector], bc: SparkContext): RDD[DenseVector] = {
//
//    val data = mat.zip(vec)
//    data.mpimapPartitions(iter => mpiMatMul(iter))
//  }

//  def mpiMatMul(data: Iterator[(Vector, DenseVector)]): Vector = {
//    data.map{
//      case (v, dv) =>
//        dv.
//        MPI.COMM_WORLD.
//    }
//  }
}
