
package org.apache.spark.blaze.mpi

import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{Task, TaskLocation}

import java.io.Serializable
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties


/**
 * A task that sends back the output to the driver application.
 *
 * See [[Task]] for more information.
 *
 * @param stageId               id of the stage this task belongs to
 * @param stageAttemptId        attempt id of the stage this task belongs to
 * @param taskBinary            broadcasted version of the serialized RDD and the function to apply on each
 *                              partition of the given RDD. Once deserialized, the type should be
 *                              (RDD[T], (TaskContext, Iterator[T]) => U).
 * @param partition             partition of the RDD this task is associated with
 * @param locs                  preferred task execution locations for locality scheduling
 * @param outputId              index of the task in this job (a job can launch tasks on only a subset of the
 *                              input RDD's partitions).
 * @param localProperties       copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 *                              The parameters below are optional:
 * @param jobId                 id of the job this task belongs to
 * @param appId                 id of the app this task belongs to
 * @param appAttemptId          attempt id of the app this task belongs to
 * @param isBarrier             whether this task belongs to a barrier stage. Spark must launch all the tasks
 *                              at the same time for a barrier stage.
 */
private[spark] class MPITask[T, U](
                                    stageId: Int,
                                    stageAttemptId: Int,
                                    taskBinary: Broadcast[Array[Byte]],
                                    partition: Partition,
                                    locs: Seq[TaskLocation],
                                    val outputId: Int,
                                    localProperties: Properties,
                                    serializedTaskMetrics: Array[Byte],
                                    jobId: Option[Int] = None,
                                    appId: Option[String] = None,
                                    appAttemptId: Option[String] = None,
                                    isBarrier: Boolean = false)
  extends Task[U](stageId, stageAttemptId, partition.index, localProperties, serializedTaskMetrics,
    jobId, appId, appAttemptId, isBarrier)
    with Serializable {

  /* MPI specific vars */
  var namespace: String = _
  var rank: Int = _

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.distinct
  }

  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTimeNs = System.nanoTime()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTimeNs = System.nanoTime() - deserializeStartTimeNs
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    func(context, rdd.iterator(partition, context))
  }

  // This is only callable on the driver side.
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ResultTask(" + stageId + ", " + partitionId + ")"
}
