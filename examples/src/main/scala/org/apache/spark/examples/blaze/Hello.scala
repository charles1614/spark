
package org.apache.spark.examples.blaze

import mpi._

object Hello {
  @throws[MPIException]
  def main(args: Array[String]): Unit = {
    MPI.Init(args)
    val myrank = MPI.COMM_WORLD.getRank
    val size = MPI.COMM_WORLD.getSize
    System.out.println("Hello world from rank " + myrank + " of " + size)
    MPI.Finalize()
  }
}
