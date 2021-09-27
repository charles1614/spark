
package org.apache.spark.examples.blaze.cg

import mpi.MPI
import org.apache.spark.examples.blaze.linalg.ArrayHelper._
import org.apache.spark.mllib.linalg.{BLAS, DenseVector, Vector}


object ConjugateGradients {
  //  val A: Matrix = Matrix.get_randomMatrixSPD(bc, size = size) * 6 - 3
  //  val b: Vector = Matrix.get_randomVector(bc, size = size) * 4 - 2

  /** Ax=b solver,
   * parameter:
   * size: rows
   * a: step length,
   * x: approximate solution
   * r: residual,
   * c: improvement this step
   * p: search direction
   */
  def cg(iterPair: Iterator[(Vector, DenseVector)]
         , size: Int, maxIter: Int = 1000, enableMPI: Boolean = true): Iterator[Double] = {

    val arr = iterPair.map(x => (x._1, x._2)).toArray
    val ALocal = arr.map(x => x._1)
    val bLocal = arr.map(x => x._2)

    val start = System.nanoTime

    MPI.Init(Array.fill(1)("null"))
    val comm = MPI.COMM_WORLD
    val rank = comm.getRank
    val rsize = comm.getSize

    // recieve full b for each rank
    var b = Array.fill(size)(0.0)
    // get full vec
    var bLocalArr = bLocal.flatMap(x => x.values)

    if (enableMPI) {
      vecGather(bLocalArr, b)
    }
    //    b.foreach(iter => print(iter + "\n"))
    // residual init
    var r, rn = b
    // search direction
    var p = r

    var a, rr, pap: Double = 0
    var c: Double = 0
    var pa: Array[Double] = Array.fill(size)(0.0)
    var paLocal: Array[Double] = Array.fill(ALocal.size)(0.0)
    var x = Array.fill(size)(0.0)

    var iter = 0
    var realIter = 0

    /** begin iteration */
    while (iter < maxIter) {
      iter = iter + 1

      //      if (rank == 0) {
      //        println(s"iter: ${iter}")
      //      }

      // calculate step a
      //      rr = r.map(iter => iter * iter).sum
      rr = r dot r

      /** distributed matrix-vector multiplication */
      paLocal = ALocal.map(v => BLAS.dot(v, new DenseVector(p)))
      vecGather(paLocal, pa)

      //      pap = p.zip(pa).map(iter => iter._1 * iter._2).sum
      pap = p dot pa
      a = rr / pap
      x = x + p * a

      //      if (rank == 0) {
      //        print(x.foreach(x => println(s"${x}")))
      //      }
      rn = r - pa * a
      c = (rn dot rn)
      //      println(c)
      if (c < 1E-9) {
        realIter = iter
        iter = maxIter
      }
      c = c / (r dot r)
      p = rn + p * c
      r = rn
    }

    MPI.Finalize()
    var stop = System.nanoTime
    print(s"elapse time is ${(stop - start) / 1E6}, iter is ${realIter}")
    x.toIterator
  }

  def vecGather(local: Array[Double], vec: Array[Double]): Unit = {
    val comm = MPI.COMM_WORLD
    val rank = comm.getRank
    val rsize = comm.getSize
    //    local.foreach(vec => print(s"${rank}: ${vec}\n"))
    //    println(s"local: ${local.size}, vec: ${vec.size}")
    MPI.COMM_WORLD.allGather(local, local.size, MPI.DOUBLE, vec, local.size, MPI.DOUBLE)
  }

  def mcg(iterPair: Iterator[(Vector, DenseVector)]
          , size: Int, maxIter: Int = 1000, enableMPI: Boolean = true): Iterator[Double] = {

    val arr = iterPair.map(x => (x._1, x._2)).toArray
    val ALocal = arr.map(x => x._1)
    val bLocal = arr.map(x => x._2)

    val start = System.nanoTime

    // recieve full b for each rank
    var b = Array.fill(size)(0.0)
    // get full vec
    var bLocalArr = bLocal.flatMap(x => x.values)

    if (enableMPI) {
      vecGather(bLocalArr, b)
    }
    //    b.foreach(iter => print(iter + "\n"))
    // residual init
    var r, rn = b
    // search direction
    var p = r

    var a, rr, pap: Double = 0
    var c: Double = 0
    var pa: Array[Double] = Array.fill(size)(0.0)
    var paLocal: Array[Double] = Array.fill(ALocal.size)(0.0)
    var x = Array.fill(size)(0.0)

    var iter = 0
    var realIter = 0

    /** begin iteration */
    while (iter < maxIter) {
      iter = iter + 1

      rr = r dot r

      /** distributed matrix-vector multiplication */
      paLocal = ALocal.map(v => BLAS.dot(v, new DenseVector(p)))
      vecGather(paLocal, pa)

      pap = p dot pa
      a = rr / pap
      x = x + p * a

      rn = r - pa * a
      c = (rn dot rn)

      if (c < 1E-9) {
        realIter = iter
        iter = maxIter
      }
      c = c / (r dot r)
      p = rn + p * c
      r = rn
    }

    var stop = System.nanoTime
    print(s"elapse time is ${(stop - start) / 1E6}, iter is ${realIter}")
    x.toIterator
  }
}
