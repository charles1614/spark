
package org.apache.spark.mpi

import org.apache.spark.blaze.deploy.mpi.NativeUtils

import scala.collection.JavaConverters._
import scala.io.Source
import org.apache.spark.internal.Logging

import java.util

class JavaLoggingWrapper extends Logging

object MPIUtil extends Logging {

  System.load(System.getenv("SPARK_HOME") + "/lib/libblaze.so")

  def setPmixEnv(): Unit = {
    for (line <- Source.fromFile("/tmp/pmixsrv.env").getLines()) {
      val key: String = line.split('=')(0)
      val value: String = line.split('=')(1)
      val map = new java.util.HashMap[String, String]()
      //      print(s"key: ${key}, value: ${value}\n")
      map.put(key, value)
      NativeUtil.setEnv(map)
    }
  }

  //  def setMPIEnv(taskDesc: TaskDescription): Unit = {
  //    import collection.JavaConverters._
  //    NativeUtil.setEnv(taskDesc.mpienv.asJava)
  //  }

  def setRank(rank: String): Unit = {
    val map = new java.util.HashMap[String, String]()
    map.put("PMIX_RANK", rank)
    logDebug(s"Executor start rank ${rank}")
    NativeUtil.setEnv(map)
  }

  def setMPIEnv(rank: String): Unit = {
    val map = new util.HashMap[String, String]()

    // pmix env
    for (line <- Source.fromFile("/tmp/pmixsrv.env").getLines()) {
      val key: String = line.split('=')(0)
      val value: String = line.split('=')(1)
//      val map = new java.util.HashMap[String, String]()
      //      print(s"key: ${key}, value: ${value}\n")
      logInfo(s"key: ${key}, value: ${value}\n")
      map.put(key, value)
    }
    // rank
    map.put("PMIX_RANK", rank)
    logDebug(s"Executor start rank ${rank}")

    // native env
    NativeUtils.setEnv(map)
    // jvm env
    val env = map.asScala.toMap ++ System.getenv.asScala ++ map.asScala.toMap
    EnvHacker.setEnv(env)
  }
}
