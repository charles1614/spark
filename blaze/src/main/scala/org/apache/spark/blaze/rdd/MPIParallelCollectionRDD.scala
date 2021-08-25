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
//  override def map[U: ClassTag](f: T => U): RDD[U] = withScope {
////    val cleanF = sc.clean(f)
//    logInfo("======== use mpi map ===========")
//    launchMPIJobNamespace()
//    super.map(f)
////    val cleanF = sc.clean(f)
////    new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.map(cleanF))
//  }

  override def launchMPIJobNamespace(): Thread = {
    val mpiJobNsThread = new Thread("MPINamespace Setup") {
      override def run: Unit = {
        setupMPIJobNamespace()
      }
    }
    try {
      mpiJobNsThread.start()
    } catch {
      case e: Exception => print(e)
      case _: Throwable => println("Got throwable exception")
    }
    mpiJobNsThread
  }
}
