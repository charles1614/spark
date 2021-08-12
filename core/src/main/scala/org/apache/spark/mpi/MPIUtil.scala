
package org.apache.spark.mpi


import java.util.{Collections, Map => JavaMap}

import scala.io.Source

import org.apache.spark.internal.Logging

object MPIUtil extends Logging {

  System.load("/home/xialb/lib/libblaze.so")

  def setPmixEnv(): Unit = {
    for (line <- Source.fromFile("/home/xialb/opt/spark/sbin/pmixsrv.env").getLines()) {
      val key: String = line.split('=')(0)
      val value: String = line.split('=')(1)
      val map = new java.util.HashMap[String, String]()
      //      print(s"key: ${key}, value: ${value}\n")
      map.put(key, value)
      NativeUtil.setEnv(map)
    }
  }

  def setRank(rank: String): Unit = {
    val map = new java.util.HashMap[String, String]()
    map.put("PMIX_RANK", rank)
    logDebug(s"Executor start rank ${rank}")
    NativeUtil.setEnv(map)
  }
}
