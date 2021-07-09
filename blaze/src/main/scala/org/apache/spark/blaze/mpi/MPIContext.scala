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

// scalastyle:off
package org.apache.spark.blaze.mpi

import java.net.URI
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.Properties

import scala.collection.Map
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{ContextCleaner, ExecutorAllocationManager, Heartbeater, SparkConf, SparkContext, SparkEnv, SparkException, SparkStatusTracker}
import org.apache.spark.blaze.rdd.{MPIParallelCollectionRDD, RDDOperationScope}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.plugin.PluginContainer
import org.apache.spark.rdd.ParallelCollectionRDD
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.{DAGScheduler, EventLoggingListener, LiveListenerBus, SchedulerBackend, TaskScheduler}
import org.apache.spark.shuffle.api.ShuffleDriverComponents
import org.apache.spark.status.AppStatusStore
import org.apache.spark.ui.{ConsoleProgressBar, SparkUI}
import org.apache.spark.util.{CallSite, Utils}
import org.apache.spark.util.logging.DriverLogger


class MPIContext {

  def this(sparkContext: SparkContext) {
    this()
    sc = sparkContext
  }

  // The call site where this SparkContext was constructed.
  private val creationSite: CallSite = Utils.getCallSite()

  def stop(): Unit = {
    // TODO: stop MPIContext
  }

  /* ------------------------------------------------------------------------------------- *
 | Private variables. These variables keep the internal state of the context, and are    |
 | not accessible by the outside world. They're mutable since we want to initialize all  |
 | of them to some neutral value ahead of time, so that calling "stop()" while the       |
 | constructor is still running is safe.                                                 |
 * ------------------------------------------------------------------------------------- */
  // MPI NameSpace and Rank info

  private var _namespace: String = _
  private var _mpienv: String = _

  // RDD need SparkContext
  private var sc: SparkContext = _
  private var _conf: SparkConf = _
  private var _eventLogDir: Option[URI] = None
  private var _eventLogCodec: Option[String] = None
  private var _listenerBus: LiveListenerBus = _
  private var _env: SparkEnv = _
  private var _statusTracker: SparkStatusTracker = _
  private var _progressBar: Option[ConsoleProgressBar] = None
  private var _ui: Option[SparkUI] = None
  private var _hadoopConfiguration: Configuration = _
  private var _executorMemory: Int = _
  private var _schedulerBackend: SchedulerBackend = _
  private var _taskScheduler: TaskScheduler = _
  private var _heartbeatReceiver: RpcEndpointRef = _
  @volatile private var _dagScheduler: DAGScheduler = _
  private var _applicationId: String = _
  private var _applicationAttemptId: Option[String] = None
  private var _eventLogger: Option[EventLoggingListener] = None
  private var _driverLogger: Option[DriverLogger] = None
  private var _executorAllocationManager: Option[ExecutorAllocationManager] = None
  private var _cleaner: Option[ContextCleaner] = None
  private var _listenerBusStarted: Boolean = false
  private var _jars: Seq[String] = _
  private var _files: Seq[String] = _
  private var _shutdownHookRef: AnyRef = _
  private var _statusStore: AppStatusStore = _
  private var _heartbeater: Heartbeater = _
  private var _resources: scala.collection.immutable.Map[String, ResourceInformation] = _
  private var _shuffleDriverComponents: ShuffleDriverComponents = _
  private var _plugins: Option[PluginContainer] = None

  private[spark] def taskScheduler: TaskScheduler = _taskScheduler

  // Methods for creating RDDs

  /**
   * Execute a block of code in a scope such that all new RDDs created in this body will
   * be part of the same scope. For more detail, see {{org.apache.spark.rdd.RDDOperationScope}}.
   *
   * @note Return statements are NOT allowed in the given body.
   */
  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](this)(body)

  /** Distribute a local Scala collection to form an RDD.
   *
   * @note Parallelize acts lazily. If `seq` is a mutable collection and is altered after the call
   *       to parallelize and before the first action on the RDD, the resultant RDD will reflect the
   *       modified collection. Pass a copy of the argument to avoid this.
   * @note avoid using `parallelize(Seq())` to create an empty `RDD`. Consider `emptyRDD` for an
   *       RDD with no partitions, or `parallelize(Seq[T]())` for an RDD of `T` with empty partitions.
   * @param seq       Scala collection to distribute
   * @param numSlices number of partitions to divide the collection into
   * @return RDD representing distributed collection
   */
  def parallelize[T: ClassTag](
                                seq: Seq[T],
                                numSlices: Int = defaultParallelism
                              ): MPIParallelCollectionRDD[T] = withScope {
    assertNotStopped()
    new MPIParallelCollectionRDD[T](sc, seq, numSlices, Map[Int, Seq[String]]())
  }

  /** Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD). */
  def defaultParallelism: Int = {
    assertNotStopped()
    taskScheduler.defaultParallelism
  }


  // In order to prevent multiple SparkContexts from being active at the same time, mark this
  // context as having started construction.
  // NOTE: this must be placed at the beginning of the SparkContext constructor.
  MPIContext.markPartiallyConstructed(this)

  val startTime = System.currentTimeMillis()

  private[spark] val stopped: AtomicBoolean = new AtomicBoolean(false)

  private[spark] def assertNotStopped(): Unit = {
    if (stopped.get()) {
      val activeContext = MPIContext.activeContext.get()
      val activeCreationSite =
        if (activeContext == null) {
          "(No active SparkContext.)"
        } else {
          activeContext.creationSite.longForm
        }
      throw new IllegalStateException(
        s"""Cannot call methods on a stopped SparkContext.
           |This stopped SparkContext was created at:
           |
           |${creationSite.longForm}
           |
           |The currently active SparkContext was created at:
           |
           |$activeCreationSite
         """.stripMargin)
    }
  }

  protected[spark] val localProperties = new InheritableThreadLocal[Properties] {
    override protected def childValue(parent: Properties) : Properties = {
      Utils.cloneProperties(parent)
    }

    override protected def initialValue(): Properties = new Properties()
  }

  private[spark] def getLocalProperty(): Properties = {
    localProperties.get()
  }

  private[spark] def getLocalProperty(key: String): String = {
    Option(localProperties.get).map(_.getProperty(key)).orNull
  }

  private[spark] def setLocalProperty(key: String, value: String): Unit = {
    if (value == null) {
      localProperties.get.remove(key)
    } else {
      localProperties.get.setProperty(key, value)
    }
  }
}

object MPIContext extends Logging {
  def getOrCreate(sparkconf: SparkConf): MPIContext = {
    new MPIContext
  }

  def getOrCreate(sparkconf: SparkConf, sparkContext: SparkContext): MPIContext = {
    new MPIContext(sparkContext)
  }


  /**
   * Lock that guards access to global variables that track SparkContext construction.
   */
  private val MPI_CONTEXT_CONSTRUCTOR_LOCK = new Object()

  /**
   * The active, fully-constructed SparkContext. If no SparkContext is active, then this is `null`.
   *
   * Access to this field is guarded by `SPARK_CONTEXT_CONSTRUCTOR_LOCK`.
   */
  private val activeContext: AtomicReference[MPIContext] =
    new AtomicReference[MPIContext](null)


  /**
   * Called at the beginning of the SparkContext constructor to ensure that no SparkContext is
   * running. Throws an exception if a running context is detected and logs a warning if another
   * thread is constructing a SparkContext. This warning is necessary because the current locking
   * scheme prevents us from reliably distinguishing between cases where another context is being
   * constructed and cases where another constructor threw an exception.
   */
  private[spark] def markPartiallyConstructed(mc: MPIContext): Unit = {
    MPI_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      assertNoOtherContextIsRunning(mc)
      contextBeingConstructed = Some(mc)
    }
  }


  /**
   * Called to ensure that no other SparkContext is running in this JVM.
   *
   * Throws an exception if a running context is detected and logs a warning if another thread is
   * constructing a SparkContext. This warning is necessary because the current locking scheme
   * prevents us from reliably distinguishing between cases where another context is being
   * constructed and cases where another constructor threw an exception.
   */
  private def assertNoOtherContextIsRunning(mc: MPIContext): Unit = {
    MPI_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      Option(activeContext.get()).filter(_ ne mc).foreach { ctx =>
        val errMsg = "Only one SparkContext should be running in this JVM (see SPARK-2243)." +
          s"The currently running SparkContext was created at:\n${ctx.creationSite.longForm}"
        throw new SparkException(errMsg)
      }

      contextBeingConstructed.filter(_ ne mc).foreach { otherContext =>
        // Since otherContext might point to a partially-constructed context, guard against
        // its creationSite field being null:
        val otherContextCreationSite =
          Option(otherContext.creationSite).map(_.longForm).getOrElse("unknown location")
        val warnMsg = "Another SparkContext is being constructed (or threw an exception in its" +
          " constructor). This may indicate an error, since only one SparkContext should be" +
          " running in this JVM (see SPARK-2243)." +
          s" The other SparkContext was created at:\n$otherContextCreationSite"
        logWarning(warnMsg)
      }
    }
  }


  /**
   * Points to a partially-constructed SparkContext if another thread is in the SparkContext
   * constructor, or `None` if no SparkContext is being constructed.
   *
   * Access to this field is guarded by `SPARK_CONTEXT_CONSTRUCTOR_LOCK`.
   */
  private var contextBeingConstructed: Option[MPIContext] = None

}
