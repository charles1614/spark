// scalastyle:off classforname
package org.apache.spark.blaze

import java.util.{Collections, Map => JavaMap}
import javax.annotation.processing.ProcessingEnvironment

object BlazeUtils {

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
      var cienv = theCaseInsensitiveEnvironment.get().asInstanceOf[JavaMap[String, String]]
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
//  @native def setEnv(): Unit
}
