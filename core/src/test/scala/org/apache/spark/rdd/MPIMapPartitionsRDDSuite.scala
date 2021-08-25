
package org.apache.spark.rdd

import org.apache.spark.{FetchFailed, HashPartitioner, SharedSparkContext, ShuffleDependency, SparkFunSuite, Success, TaskContext}
import org.apache.spark.scheduler.{DAGSchedulerEvent, DAGSchedulerEventProcessLoop, ExecutorKilled, ExecutorLost, JobListener, JobSubmitted, MyRDD}
import org.apache.spark.mpi.{MPIRun, MPIUtil, MPIUtilSuite, NativeUtil}
import org.apache.spark.scheduler.DAGSchedulerSuite.makeMapStatus
import org.apache.spark.util.CallSite

import java.util.Properties

class MPIMapPartitionsRDDSuite extends SparkFunSuite with SharedSparkContext {

  test("basic operation") {
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(nums.getNumPartitions == 2)
    nums.mpimap(i => i).collect()
    while(true) {
      Thread.sleep(1000)
    }
  }

  test("mpirun") {
    System.load("/home/xialb/lib/libblaze.so")
    val s = NativeUtil.namespaceQuery()
    val split = s.split(",")
    if (!split(0).isEmpty) {
      NativeUtil.namespaceFinalize(split(0))
    }
    val app = new Array[String](4)
    app(0) = "prun"
    app(1) = "-n"
    app(2) = "2"
    app(3) = "hostname"
    val prun = new Thread(
      () => {
        val mpiRun = new MPIRun()
        val rc = mpiRun.exec(app)
      }
    )
    prun.start()
    assert(prun.getState == Thread.State.RUNNABLE)
  }

  test("finalize namespace") {
    System.load("/home/xialb/lib/libblaze.so")
    val s = NativeUtil.namespaceQuery()
    val split = s.split(",")
    print(s)
    if (!split(0).isEmpty) {
      NativeUtil.namespaceFinalize(split(0))
    }
  }

  test("mpimap") {
    System.load("/home/xialb/lib/libblaze.so")
    sc.setLogLevel("INFO")
    sc.parallelize(0 to 1, 2).mpimap(i => print(i)).barrier().mapPartitions(iter => iter)
    while(true)
      Thread.sleep(1000)
  }


}
