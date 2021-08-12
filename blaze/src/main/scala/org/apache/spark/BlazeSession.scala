
package org.apache.spark

import java.io.Closeable

import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.util.{CallSite, Utils}


/**
 * The entry point to programming Spark with MPI extensions
 */
@DeveloperApi
class BlazeSession private(
                            @transient val blazeContext: BlazeContext)
  extends Serializable with Closeable with Logging {

  private val creationCallSite: CallSite = Utils.getCallSite()

  def version: String = SPARK_VERSION

  blazeContext.assertNotStopped()

  /* ----------------------- *
 |  Session-related state  |
 * ----------------------- */

  /**
   * stop underlying sparkContext
   */
  def stop(): Unit = {
    blazeContext.stop()
    mpiNamespaceStop()
  }

  def mpiNamespaceStop(): Unit = {

  }

  override def close(): Unit = stop()
}

object BlazeSession extends Logging {

  /**
   * class Builder for Blaze
   */
  class Builder extends Logging {

    private[this] val options = new mutable.HashMap[String, String]()

    private[this] var userSuppliedBlazeContext: Option[BlazeContext] = None

    private[spark] def blazeContext(blazeContext: BlazeContext): Builder = synchronized {
      userSuppliedBlazeContext = Option(blazeContext)
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Long): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Double): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Boolean): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a list of config options based on the given `SparkConf`.
     *
     * @since 2.0.0
     */
    def config(conf: SparkConf): Builder = synchronized {
      conf.getAll.foreach { case (k, v) => options += k -> v }
      this
    }

    /**
     * Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to
     * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
     *
     * @since 2.0.0
     */
    def master(master: String): Builder = config("spark.master", master)

    def appName(name: String): Builder = config("spark.app.name", name)

    private def assertOnDriver() = {
      // TODO: only TaskContest be examined, MPITaskContext also need to be examined
      if (Utils.isTesting && TaskContext.get() != null) {
        // we're accessing it during task execution, fail.
        throw new IllegalStateException(
          "BlazeSession should only be created and accessed on the driver"
        )
      }
    }

    /**
     * Getting an existing [[BlazeSession]] or, if there is not an existing one, creates a new
     * one based on the options set in the builder
     *
     * This method first checks whether there is a valid thread-local BlazeSession,
     * and if yes, return that one. It then checks whether there is a valid global
     * default BlazeSession exists, the method creates a new SparkSession and assigns
     * the newly created BlazeSession as the global default.
     *
     * In case an existing BlazeSession is returned, the non-static config options specified
     * in this builder will be applied to the existing BlazeSession.
     */
    def getOrCreate(): BlazeSession = {
      val sparkConf = new SparkConf()

      options.foreach { case (k, v) => sparkConf.set(k, v) }

      assertOnDriver()

      val blazecontext: BlazeContext = userSuppliedBlazeContext.getOrElse {
        if (!sparkConf.contains("spark.app.name")) {
          sparkConf.setAppName(java.util.UUID.randomUUID().toString)
        }
        BlazeContext.getOrCreate(sparkConf)
      }
      new BlazeSession(blazecontext)
    }
  }

  /**
   * Create [[BlazeSession.Builder]] for constructing a  [[BlazeSession]]
   */
  def builder: Builder = {
    new Builder()
  }
}
