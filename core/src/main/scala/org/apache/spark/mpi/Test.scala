
package org.apache.spark.mpi

import org.apache.spark.blaze.deploy.mpi.NativeUtils

import scala.collection.JavaConverters._
import java.util

object Test {
  def main(args: Array[String]): Unit = {
    //    System.load("/home/xialb/lib/libblaze.so")
    //    val s = NativeUtil.namespaceQuery()
    //    println(s)
    //    val split = s.split(",")
    //    if (!split(0).isEmpty) {
    //      println(split(0).isEmpty)
    //      new Thread() {
    //        NativeUtil.namespaceFinalize(split(0))
    //      }.start()
    //    }
    //    Thread.sleep(3000)

    //    val app = new Array[String](6)
    //    app(0) = "prun"
    //    app(1) = "-n"
    //    app(2) = "2"
    //    app(3) = "--map-by"
    //    app(4) = ":OVERSUBSCRIBE"
    //    app(3) = "hostname"
    //    val mpiRun = new MPIRun()
    //    val rc = mpiRun.exec(app)
    val map = new util.HashMap[String, String]()
    //    for (line <- Source.fromFile("/tmp/pmixsrv.env").getLines()) {
    //      val key: String = line.split('=')(0)
    //      val value: String = line.split('=')(1)
    //      //      val map = new java.util.HashMap[String, String]()
    //      //      print(s"key: ${key}, value: ${value}\n")
    //      logInfo(s"key: ${key}, value: ${value}\n")
    //      map.put(key, value)
    //    }
    // rank
    System.out.println(s"JVM: ${System.getenv("HOME")}")
    map.put("HOME", "/home/libin")

    // jvm env
    val env = map.asScala.toMap ++ System.getenv.asScala ++ map.asScala.toMap
    EnvHacker.setEnv(env)
    NativeUtils.setEnv(map)
    System.out.println(s"JVM: ${System.getenv("HOME")}")
  }
}
