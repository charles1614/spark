
package org.apache.spark.blaze.ompi

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils


class MPIConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {

  /** Create a MPIConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

//  @transient private lazy val reader: ConfigReader = {
//    val _reader = new ConfigReader(new SparkConfigProvider(settings))
//    _reader.bindEnv((key: String) => Option(getenv(key)))
//    _reader
//  }

  if (loadDefaults) {
    loadFromSystemProperties(false)
  }

  private[spark] def loadFromSystemProperties(silent: Boolean): MPIConf = {
    // Load any spark.* system properties
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("pmix.")) {
      set(key, value, silent)
    }
    this
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): MPIConf = {
    set(key, value, false)
  }

  private[spark] def set(key: String, value: String, silent: Boolean): MPIConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
//    if (!silent) {
//      logDeprecationWarning(key)
//    }
    settings.put(key, value)
    this
  }

}

object MPIConf extends Logging {

}


