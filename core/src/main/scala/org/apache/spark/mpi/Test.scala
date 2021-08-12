
package org.apache.spark.mpi

object Test {
  def main(args: Array[String]): Unit = {
    System.load("/home/xialb/lib/libblaze.so")
//    System.loadLibrary("blaze")
    MPIUtil.setPmixEnv()
//    print(NativeUtil.namespaceQuery())
//    NativeUtil.namespaceQuery()
  }
}
