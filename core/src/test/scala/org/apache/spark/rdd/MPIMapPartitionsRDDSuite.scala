
package org.apache.spark.rdd

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.mpi.{MPIRun, NativeUtil}

class MPIMapPartitionsRDDSuite extends SparkFunSuite with SharedSparkContext {

  test("basic operation") {
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 1)
    assert(nums.getNumPartitions == 1)
    nums.mpimapPartitions(iter => iter.map(print(_))).collect()
//    nums.mpimap{
//      i => print(i)
//    }.collect()
    while(true) {
      Thread.sleep(1000)
    }
  }

  test("mpirun") {
    System.load(System.getenv("SPARK_HOME") + "/lib/libblaze.so")
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
    System.load(System.getenv("SPARK_HOME") + "/lib/libblaze.so")
    val s = NativeUtil.namespaceQuery()
    val split = s.split(",")
    print(s)
    if (!split(0).isEmpty) {
      NativeUtil.namespaceFinalize(split(0))
    }
  }

  test("mpimap") {
//    System.load("/home/xialb/lib/libblaze.so")
    sc.setLogLevel("INFO")
    sc.parallelize(0 to 1, 2).mpimap(i => print(i)).barrier().mapPartitions(iter => iter)
    while(true)
      Thread.sleep(1000)
  }


}
