
package org.apache.spark.mpi

import scala.collection.JavaConverters._

import scala.io.Source
import org.apache.spark.internal.Logging

import java.util

class JavaLoggingWrapper extends Logging

object MPIUtil extends Logging {

  System.load("/home/xialb/lib/libblaze.so")

  def setPmixEnv(): Unit = {
    for (line <- Source.fromFile("/tmp/pmixsrv.env").getLines()) {
      val key: String = line.split('=')(0)
      val value: String = line.split('=')(1)
      val map = new java.util.HashMap[String, String]()
      //      print(s"key: ${key}, value: ${value}\n")
      map.put(key, value)
      NativeUtil.setEnv(map)
      //      EnvHacker.setEnv(scala.collection.immutable.Map(key->value))
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
    // for JVM env
    EnvHacker.setEnv(scala.collection.immutable.Map("PMIX_RANK" -> rank))
  }

  def setMPIEnv(rank: String): Unit = {
    val map = new util.HashMap[String, String]()

    // pmix env
    for (line <- Source.fromFile("/tmp/pmixsrv.env").getLines()) {
      val key: String = line.split('=')(0)
      val value: String = line.split('=')(1)
//      val map = new java.util.HashMap[String, String]()
      //      print(s"key: ${key}, value: ${value}\n")
      map.put(key, value)
    }
    // rank
    map.put("PMIX_RANK", rank)
    logDebug(s"Executor start rank ${rank}")

    // native env
    NativeUtil.setEnv(map)
    // jvm env
    val env = map.asScala.toMap ++ System.getenv.asScala
    EnvHacker.setEnv(env)
  }
}
