
package org.apache.spark.examples.blaze

import scala.math.sqrt
import mpi.{CartComm, Datatype, MPI}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.derby.iapi.util.ByteArray
import org.apache.log4j.Logger
import org.apache.spark.BlazeSession
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{BLAS, DenseMatrix, Matrix, Vectors}
import org.apache.spark.rdd.RDD

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

object MPIOperators {

  @transient lazy val log = Logger.getLogger(getClass.getName)

  def mpiMultiply(a: DenseMatrix, b: DenseMatrix): DenseMatrix = {

    MPI.Init(Array.fill(1)("null"))

    val comm = MPI.COMM_WORLD
    val rank = comm.getRank
    val size = comm.getSize

    val start = System.nanoTime

    // change to var
    var aa = a
    var bb = b

    // matrix element counts
    var counts = a.numRows * a.numCols

    var c: DenseMatrix = new DenseMatrix(a.numRows, a.numCols, Array.ofDim(counts))

    /** initialize cart Comm */
    val dims: Array[Int] = Array.fill(2)(sqrt(size).toInt)
    val period: Array[Boolean] = Array.fill(2)(true)
    // recorder is true, return new rank
    val cart: CartComm = comm.createCart(dims, period, true)

    val rank2d = cart.getRank()
    val coords = cart.getCoords(rank2d)

    /** calculate neighbour rank */
    // >0 upwards(forwards) <0 downwards(backwards)
    val rcol = cart.shift(0, -1)
    val rrow = cart.shift(1, -1)

    val byteBuff = ByteBuffer.allocateDirect(2000)

    /** to optimize */
    var waste_time, comm_time, comm_start, comm_end: Long = 0

    /** iteration nums (partitions) */
    for (i <- 0 until dims(0)) {

      val start_ser = System.nanoTime
      val sa = serialize(aa)
      val sb = serialize(bb)
      val end_ser = System.nanoTime

      val comm_start = System.nanoTime
      if (i == 0) {
        /** initialize matrix for iter 0 */
        val scol = cart.shift(0, -coords(1))
        val srow = cart.shift(1, -coords(0))

        /** col movement for aa Matrix */
        cart.sendRecvReplace(sa, sa.length, MPI.BYTE, srow.getRankDest, 2, srow.getRankSource, 2)
        cart.sendRecvReplace(sb, sb.length, MPI.BYTE, scol.getRankDest, 1, scol.getRankSource, 1)
      } else {
        cart.sendRecvReplace(sa, sa.length, MPI.BYTE, rrow.getRankDest, 2, rrow.getRankSource, 2)
        cart.sendRecvReplace(sb, sb.length, MPI.BYTE, rcol.getRankDest, 1, rcol.getRankSource, 1)
      }
      val comm_end = System.nanoTime
      comm_time = comm_time + (comm_end - comm_start)

      val start_des = System.nanoTime
      aa = deserialize(sa)
      bb = deserialize(sb)
      val end_des = System.nanoTime
      waste_time = waste_time + (end_des - start_des + end_ser - start_ser)

      BLAS.gemm(1, aa, bb, 1, c)
    }

    cart.barrier()

    val end = System.nanoTime
    println(s"ser_der time is ${waste_time / 1000000}ms," +
      s" comm time is ${comm_time / 1000000}," +
      s" totol time is ${(end - start) / 1000000}ms")

    MPI.Finalize()
        print(s"rank${rank2d}\n${c.values(0)}\n")
    c
  }

  def serialize(mat: DenseMatrix): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(mat)
    bos.toByteArray
  }

  def deserialize(bytes: Array[Byte]): DenseMatrix = {
    // TODO: Use ByteBuffer to optimize
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject().asInstanceOf[DenseMatrix]
  }

}
