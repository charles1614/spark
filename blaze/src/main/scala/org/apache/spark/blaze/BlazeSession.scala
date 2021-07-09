/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.blaze

import java.io.Closeable

import scala.collection.mutable

import org.apache.spark.{SPARK_VERSION, SparkConf, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.blaze.mpi.MPIContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.{CallSite, Utils}


/**
 * The entry point to programming Spark with MPI extensions
 */
@DeveloperApi
class BlazeSession private(
                            @transient val sparkContext: SparkContext)(
                            @transient val mpiContext: MPIContext
                          ) extends Serializable with Closeable with Logging {

  private val creationCallSite: CallSite = Utils.getCallSite()

  def version: String = SPARK_VERSION

  sparkContext.assertNotStopped()

  /* ----------------------- *
 |  Session-related state  |
 * ----------------------- */

  /**
   * stop underlying sparkContext
   */
  def stop(): Unit = {
    sparkContext.stop()
    mpiContext.stop()
  }


  override def close(): Unit = stop()
}

object BlazeSession extends Logging {

  /**
   * class Builder for Blaze
   */
  class Builder extends Logging {

    private[this] val options = new mutable.HashMap[String, String]()

    private[this] var userSuppliedSparkContext: Option[SparkContext] = None
    private[this] var userSuppliedMPIContext: Option[MPIContext] = None

    private[spark] def sparkContext(sparkContext: SparkContext): Builder = synchronized {
      userSuppliedSparkContext = Option(sparkContext)
      this
    }

    private[spark] def mpiContext(mpiContext: MPIContext): Builder = synchronized {
      userSuppliedMPIContext = Option(mpiContext)
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

    def appName(name: String): Builder = config("blaze.app.name", name)

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

      options.foreach { case (k, v) => sparkConf.set(k, v ) }

      assertOnDriver()

      val sparkcontext: SparkContext = userSuppliedSparkContext.getOrElse {
        if (!sparkConf.contains("spark.app.name")) {
          sparkConf.setAppName(java.util.UUID.randomUUID().toString)
        }
        SparkContext.getOrCreate(sparkConf)
      }

      val mpicontext: MPIContext = userSuppliedMPIContext.getOrElse {
        if (!sparkConf.contains("spark.app.name")) {
          sparkConf.setAppName(java.util.UUID.randomUUID().toString)
        }
        MPIContext.getOrCreate(sparkConf, sparkcontext)
      }

      new BlazeSession(sparkcontext)(mpicontext)
    }
  }

  /**
   * Create [[BlazeSession.Builder]] for constructing a  [[BlazeSession]]
   */
  def builder: Builder = {
    new Builder()
  }

}
