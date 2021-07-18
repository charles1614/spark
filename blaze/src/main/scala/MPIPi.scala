
import java.util

import mpi.{MPI, MPIException}

import org.apache.spark.SparkConf
import org.apache.spark.blaze.{BlazeSession, BlazeUtils}
import org.apache.spark.blaze.deploy.mpi.NativeUtils
import org.apache.spark.blaze.ompi.MPIConf

// scalastyle:off println

/** Computes an approximation to pi */
object MPIPi {

  @throws[MPIException]
  private def mpiop(mpargs: Array[String]): Unit = {
    //            val map = new util.HashMap[String, String]()
    //            BlazeUtils.setJavaEnv(map)
    //        print(sys.env)
    //        setEnv(map)
    //    //    getEnv()

    println(s"========== ${mpargs(0)} ===============")

    BlazeUtils.setPmixEnv()
    val map = new util.HashMap[String, String]()
    //    map.put("PMIX_RANK", mpargs(0))
    map.put("PMIX_RANK", mpargs(0))
    NativeUtils.setEnv(map)

    //    val stringToString = NativeUtils.getEnv("PMIX")
    //    import collection.JavaConverters._
    //    for (s <- stringToString.asScala) {
    //      println(s"key: ${s._1}, value: ${s._2}")
    //    }


    //    MPI.Init(mpargs)
    MPI.InitThread(mpargs, MPI.THREAD_MULTIPLE)
    //    getEnv()
    //    while (true) {
    //      sleep(1000)
    //    };
    val myrank = MPI.COMM_WORLD.getRank
    val size = MPI.COMM_WORLD.getSize
    //    getEnv()
    print("Hello world from rank " + myrank + " size of " + size + "\n")
    MPI.Finalize()
    //    NativeUtils.test()

  }


  def main(args: Array[String]): Unit = {

    val a = 1
    val conf = new SparkConf()
      .set("spark.master", "spark://192.168.32.197:7077")
    val mconf = new MPIConf()
      .set("spark.master", "spark://192.168.32.197:7077")

    conf.set("spark.executor.cores", 1.toString).set("spark.executor.instances", "4")
    mconf.set("spark.executor.cores", 1.toString).set("spark.executor.instances", "4")

    val stringToString = new util.HashMap[String, String]()
    stringToString.put("key", "value")
    stringToString.put("key1", "value1")

    val blaze = BlazeSession
      .builder
      .appName("blazePi")
      .config(conf)
      //      .master("local[*]")
      .master("spark://192.168.32.197:7077")
      .getOrCreate()

    val slices = if (args.length > 0) args(0).toInt else 2
    //    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    //    val count = blaze.sparkContext.parallelize(1 until n, slices).map { i =>
    //      //      mpiop(args)
    //      val x = random * 2 - 1
    //      val y = random * 2 - 1
    //      if (x * x + y * y <= 1) 1 else 0
    //    }.reduce(_ + _)
    blaze.sparkContext.parallelize(0 until 2, slices).map(i => {
      val argv = Array(i.toString)
      mpiop(argv)
    }).collect()
    //
    //        val mc = blaze.mpiContext
    //        mc.setLogLevel("INFO")
    //        mc.parallelize(0 until 3, slices).map(i => {
    //          val argv = Array(i.toString)
    //          mpiop(argv)
    //        }).collect()

    //    val argv = Array(0.toString)
    //    mpiop(argv)

    //    print(s"Pi is roughly ${4.0 * count / (n - 1)} \n")
    blaze.stop()
  }
}
