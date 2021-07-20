
package org.apache.spark.blaze

import org.apache.spark.blaze.deploy.mpi.NativeUtils

import java.util.{Collections, Map => JavaMap}
import scala.io.Source
import org.apache.spark.blaze.deploy.mpi.NativeUtils
import org.apache.spark.internal.Logging

// scalastyle:off classforname
// scalastyle:off println
object BlazeUtils extends Logging{

  def setJavaEnv(nenv: JavaMap[String, String]): Unit = {
    try {
      val processEnvrionmnetClass = Class.forName("java.lang.ProcessEnvironment")
      val theEnvironmentField = processEnvrionmnetClass.getDeclaredField("theEnvironment")
      //      val environ = processEnvrionmnetClass.getDeclaredMethod("environ")
      //      environ.setAccessible(true)
      //      val cons = processEnvrionmnetClass.getDeclaredConstructor()
      //      cons.setAccessible(true)
      //      val obj = cons.newInstance()
      //      val environval = environ.invoke(obj)
      //      print(environval+ "=============0")
      theEnvironmentField.setAccessible(true)
      val envs = theEnvironmentField.get(null).asInstanceOf[JavaMap[String, String]]
      envs.putAll(nenv)
      val theCaseInsensitiveEnvironment =
        processEnvrionmnetClass.getDeclaredField("theCaseInsensitiveEnvironment")
      theCaseInsensitiveEnvironment.setAccessible(true)
      var cienv = theCaseInsensitiveEnvironment.get(null).asInstanceOf[JavaMap[String, String]]
      cienv.putAll(nenv)
    } catch {
      case e: NoSuchFieldException =>
        try {
          val classes = classOf[Collections].getDeclaredClasses()
          val env = System.getenv()
          for (cl <- classes) {
            if (cl.getName == "java.util.Collections$UnmodifiableMap") {
              val filed = cl.getDeclaredField("m")
              filed.setAccessible(true)
              val obj = filed.get(env)
              var map = obj.asInstanceOf[JavaMap[String, String]]
              map.clear()
              map.putAll(nenv)
            }
          }
        } catch {
          case e2: Exception => e2.printStackTrace()
        }

      case e1: Exception => e1.printStackTrace()
    }
  }

  import java.util

  def libpath(): Unit = {
    val javaLibPath = System.getProperty("java.library.path")
    val envVars = System.getenv
    println(envVars.get("PATH"))
    println(javaLibPath)
    import scala.collection.JavaConverters._
    for (k <- envVars.keySet.asScala) {
      println("examining " + k)
      if (envVars.get(k) == javaLibPath) println(k)
    }
  }


  def setPmixEnv(): Unit = {
    for (line <- Source.fromFile("/home/xialb/opt/spark/pmixsrv.env").getLines()) {
      val key: String = line.split('=')(0)
      val value: String = line.split('=')(1)
      val map = new util.HashMap[String, String]()
//      print(s"key: ${key}, value: ${value}\n")
      map.put(key, value)
      NativeUtils.setEnv(map)
    }
  }

  def setRank(rank: String): Unit = {
    val map = new util.HashMap[String, String]()
    map.put("PMIX_RANK", rank)
    logDebug(s"executor start rank ${rank}")
    NativeUtils.setEnv(map)
  }
}
