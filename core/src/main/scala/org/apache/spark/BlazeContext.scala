
package org.apache.spark


import java.util.concurrent.atomic.AtomicReference

import scala.collection.Map
import scala.reflect.ClassTag

import org.apache.spark.blaze.deploy.mpi.MPIContextNative.{getNamespace, getRanks, getSrvPeer}
import org.apache.spark.blaze.ompi.Peer
import org.apache.spark.blaze.rdd.MPIParallelCollectionRDD
import org.apache.spark.internal.Logging
import org.apache.spark.util.{CallSite, Utils}



class BlazeContext(config: SparkConf) extends SparkContext(config) with Serializable {

  // TODO: stop BlazeContext, deregister namespace
  //  def stop(): Unit = {
  //  }

  // The call site where this SparkContext was constructed.
  private val creationSite: CallSite = Utils.getCallSite()

  /* ------------------------------------------------------------------------------------- *
| Private variables. These variables keep the internal state of the context, and are    |
| not accessible by the outside world. They're mutable since we want to initialize all  |
| of them to some neutral value ahead of time, so that calling "stop()" while the       |
| constructor is still running is safe.                                                 |
* ------------------------------------------------------------------------------------- */
  // MPI NameSpace and Rank info

  private[spark] var _namespace: String = _
  private var _ranks: Int = _
  private var _mpienv: String = _

  // Spark

  private var bc: BlazeContext = _

  /* ------------------------------------------------------------------------------------- *
| Accessors and public fields. These provide access to the internal state of the        |
| context.                                                                              |
* ------------------------------------------------------------------------------------- */

  def srvPeer: Peer = {
    getSrvPeer();
  }

  def myPeer: Peer = {
    new Peer(_namespace, _ranks)
  }

  def namespace: String = {
    _namespace = getNamespace();
    _namespace
  }

  def ranks: Int = {
    _ranks = getRanks();
    _ranks
  }


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
  override def parallelize[T: ClassTag](
                                seq: Seq[T],
                                numSlices: Int = defaultParallelism
                              ): MPIParallelCollectionRDD[T] = withScope {
    assertNotStopped()
    new MPIParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
  }

}

object BlazeContext extends Logging {

  private val VALID_LOG_LEVELS =
    Set("ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN")

  def getOrCreate(sparkconf: SparkConf): BlazeContext = {
    new BlazeContext(sparkconf)
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
  private val activeContext: AtomicReference[BlazeContext] =
    new AtomicReference[BlazeContext](null)


  /**
   * Called at the beginning of the SparkContext constructor to ensure that no SparkContext is
   * running. Throws an exception if a running context is detected and logs a warning if another
   * thread is constructing a SparkContext. This warning is necessary because the current locking
   * scheme prevents us from reliably distinguishing between cases where another context is being
   * constructed and cases where another constructor threw an exception.
   */
  private[spark] def markPartiallyConstructed(mc: BlazeContext): Unit = {
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
  private def assertNoOtherContextIsRunning(bc: BlazeContext): Unit = {
    MPI_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      Option(activeContext.get()).filter(_ ne bc).foreach { ctx =>
        val errMsg = "Only one SparkContext should be running in this JVM (see SPARK-2243)." +
          s"The currently running SparkContext was created at:\n${ctx.creationSite.longForm}"
        throw new SparkException(errMsg)
      }

      contextBeingConstructed.filter(_ ne bc).foreach { otherContext =>
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
  private var contextBeingConstructed: Option[BlazeContext] = None

}

