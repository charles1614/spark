// scalastyle:off

package org.apache.spark.blaze.rdd

import scala.collection.Map
import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.blaze.deploy.mpi.{MPIRun, NativeUtils}
import org.apache.spark.rdd.{MapPartitionsRDD, ParallelCollectionRDD, RDD}

import scala.sys.exit

private[spark] class MPIParallelCollectionRDD[T: ClassTag](
    sc: SparkContext,
    @transient private val data: Seq[T],
    numSlices: Int,
    locationPrefs: Map[Int, Seq[String]]
    ) extends ParallelCollectionRDD[T] (
      sc,
      data,
      numSlices,
      locationPrefs) with Serializable {

  // TODO: how to map
  override def map[U: ClassTag](f: T => U): RDD[U] = withScope {
//    val cleanF = sc.clean(f)
    logInfo("======== use mpi map ===========")
    launchMPIJobNamespace()
    super.map(f)
//    new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.map(cleanF))
  }

  def launchMPIJobNamespace(): Thread = {
    val mpiJobNsThread = new Thread {
      override def run: Unit = {
        setupMPIJobNamespace()
      }
    }
    mpiJobNsThread.start()
    mpiJobNsThread
  }

  def finalizeMPINamespace(): Unit = {
    var ns = NativeUtils.namespaceQuery()
    NativeUtils.namespaceFinalize(ns);
  }

  def setupMPIJobNamespace(): Unit ={
//    NativeUtils.loadLibrary("")
    val partitions = super.getPartitions
    // TODO: mpi use partitions.length as mpi job cores
    val np = partitions.length
    logInfo(s"attempt to start ${np} cores for MPIJob")
    val cmd = Array[String]("prun", "-n", np.toString, "hostname")
    val rc = MPIRun.launch(cmd)
    if (0 !=  rc) {
      logError("setupMPIJobNamespace Failure")
      exit(rc)
    }
  }

}
