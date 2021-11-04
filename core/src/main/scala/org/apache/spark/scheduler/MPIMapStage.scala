
package org.apache.spark.scheduler

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

private[spark] class MPIMapStage(
  id: Int,
  rdd: RDD[_],
  val func: (TaskContext, Iterator[_]) => _,
  val partitions: Array[Int],
  parents: List[Stage],
  firstJobId: Int,
  callSite: CallSite,
  resourceProfileId: Int)
  extends Stage (id, rdd, partitions.length, parents, firstJobId, callSite, resourceProfileId)
  {

    private[this] var _mpiStageJobs: List[ActiveJob] = Nil

    def mpiStageJobs: Seq[ActiveJob] = _mpiStageJobs

    /** Adds the job to the active job list. */
    def addActiveJob(job: ActiveJob): Unit = {
      _mpiStageJobs = job :: _mpiStageJobs
    }

    /** Removes the job from the active job list. */
    def removeActiveJob(job: ActiveJob): Unit = {
      _mpiStageJobs = _mpiStageJobs.filter(_ != job)
    }

    /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
    override def findMissingPartitions(): Seq[Int] = {
//      mapOutputTrackerMaster
//        .findMissingPartitions(shuffleDep.shuffleId)
//        .getOrElse(0 until numPartitions)
      0 until numPartitions
    }
  }
