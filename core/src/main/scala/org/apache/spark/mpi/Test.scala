
package org.apache.spark.mpi

object Test {
  def main(args: Array[String]): Unit = {
    System.load("/home/xialb/lib/libblaze.so")
    val s = NativeUtil.namespaceQuery()
    println(s)
    val split = s.split(",")
    if (!split(0).isEmpty) {
      println(split(0).isEmpty)
      new Thread() {
        NativeUtil.namespaceFinalize(split(0))
      }.start()
    }
    Thread.sleep(3000)
//    val app = new Array[String](6)
//    app(0) = "prun"
//    app(1) = "-n"
//    app(2) = "2"
//    app(3) = "--map-by"
//    app(4) = ":OVERSUBSCRIBE"
//    app(3) = "hostname"
//    val mpiRun = new MPIRun()
//    val rc = mpiRun.exec(app)
  }
}
