
package org.apache.spark.examples.blaze

import scala.math.sqrt
import mpi.{CartComm, Datatype, MPI}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.derby.iapi.util.ByteArray
import org.apache.log4j.Logger
import org.apache.spark.mllib.linalg.{BLAS, DenseMatrix, Matrix}

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

object MPIOperators {

  @transient lazy val log = Logger.getLogger(getClass.getName)

  def mpiMultiply(a: DenseMatrix, b: DenseMatrix): DenseMatrix = {

    // temp
//    val blocks: Seq[((Int, Int), DenseMatrix)] = Seq(
//      ((0, 0), new DenseMatrix(2, 2, Array(1.0, 0.0, 0.0, 2.0))),
//      ((0, 1), new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 0.0))),
//      ((1, 0), new DenseMatrix(2, 2, Array(3.0, 0.0, 1.0, 1.0))),
//      ((1, 1), new DenseMatrix(2, 2, Array(1.0, 2.0, 0.0, 1.0))))
    //

    MPI.Init(Array.fill(1)("null"))

    val comm = MPI.COMM_WORLD
    val rank = comm.getRank
    val size = comm.getSize

    // change to var
    var aa = a
    var bb = b
//    rank match {
//      case 0 => aa = blocks(0)._2
//      case 1 => aa = blocks(1)._2
//      case 2 => aa = blocks(2)._2
//      case 3 => aa = blocks(3)._2
//    }
//    bb = aa

    var c: DenseMatrix = new DenseMatrix(2, 2, Array(0.0, 0.0, 0.0, 0.0))

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

    for (i <- 0 until 2) {
      val sa = serialize(aa)
      val sb = serialize(bb)
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
      aa = deserialize(sa)
      bb = deserialize(sb)
      BLAS.gemm(1, aa, bb, 1, c)
    }
    cart.barrier()
    MPI.Finalize()
    print(s"rank${rank2d}\n${c.toString}\n")
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


  def main(args: Array[String]): Unit = {
    val blocks: Seq[((Int, Int), DenseMatrix)] = Seq(
      ((0, 0), new DenseMatrix(2, 2, Array(1.0, 0.0, 0.0, 2.0))),
      ((0, 1), new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 0.0))),
      ((1, 0), new DenseMatrix(2, 2, Array(3.0, 0.0, 1.0, 1.0))),
      ((1, 1), new DenseMatrix(2, 2, Array(1.0, 2.0, 0.0, 1.0))))
    val ret = mpiMultiply(blocks(0)._2, blocks(0)._2)
    Thread.sleep(1000)
    //    val a = blocks.map {
    //      case ((r, c), mat) =>
    //        mpiMultiply(mat, mat)
    //    }
    //        a.map(m => print(m.toString))
  }
}
