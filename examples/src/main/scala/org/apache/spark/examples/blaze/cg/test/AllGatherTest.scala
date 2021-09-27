
package org.apache.spark.examples.blaze.cg.test

import mpi.MPI

object AllGatherTest {
  def main(args: Array[String]): Unit = {
    val source1 = Array.fill(1)(1)
    val source2 = Array.fill(1)(2)
    val source3 = Array.fill(1)(3)
    val dest = Array.fill(3)(0)
    MPI.Init(Array.fill(1)("null"))
    val comm = MPI.COMM_WORLD
    val rank = comm.getRank
    val rsize = comm.getSize
    if (rank == 0) {
      comm.allGather(source1, 1, MPI.INT, dest, 1, MPI.INT)
      print(dest(0))
      print(dest(1))
//      print(dest(2))
    } else if (rank == 1) {
      comm.allGather(source2, 1, MPI.INT, dest, 1, MPI.INT)
    } else {
      comm.allGather(source3, 1, MPI.INT, dest, 1, MPI.INT)
    }
    MPI.Finalize()
  }
}
