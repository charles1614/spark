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

// scalastyle:off println
package org.apache.spark.examples

import scala.util.Random

import mpi.{MPI, MPIException}

import org.apache.spark.{BlazeSession}

/** Computes an approximation to pi */
object MPIP1 {

  @throws[MPIException]
  def mpiop(mpargs: Array[String]): Double = {

    MPI.Init(mpargs)

    val myrank = MPI.COMM_WORLD.getRank
    val size = MPI.COMM_WORLD.getSize

    val points = 100000;
    var ppn: Int = 0
    if (myrank != 0) {
      ppn = points / size
    } else {
      ppn = points - (points / size) * (size - 1)
    }
    var cnt = 0
    println(s"rank: ${myrank} ppn:${ppn}")
    val random = new Random(myrank)
    for (i <- 0 until ppn) {
      val x = random.nextDouble() * 2 - 1
      val y = random.nextDouble() * 2 - 1
      if (x * x + y * y <= 1) cnt += 1
    }
    val send = Array[Int](cnt)
    var recv = Array[Int](cnt)
    val tag = 50
    MPI.COMM_WORLD.send(send, 1, MPI.INT, 0, 50)
    MPI.COMM_WORLD.reduce(send, recv, 1, MPI.INT, MPI.SUM, 0)

    var res: Double = 0
    if (myrank == 0) {
      res = (recv(0).toDouble / points * 4)
    }
    MPI.Finalize()
    res
  }


  def main(args: Array[String]): Unit = {

    //    val conf = new SparkConf()
    //      .set("spark.executor.cores", "1")
    //      .set("spark.task.cpus", "1")
    //      .set("spark.master", "spark://192.168.32.197:7077")
    //      .setJars(Array[String]("/home/xialb/opt/spark/examples/target/scala-2.12/jars/spark-examples_2.12-3.0.3-SNAPSHOT.jar"))

    val blaze = BlazeSession
      .builder
      .appName("blazePi")
//      .master("local[*]")
      //      .config(conf)
      .getOrCreate()

    val start = System.nanoTime()
    val bc = blaze.blazeContext

    bc.setLogLevel("INFO")


    val data = bc.parallelize(0 until 3, 3)

    val res = data.mpimap { i =>
      println("ok run on exec")
      val argv = Array(i.toString)
      mpiop(argv)
    }.reduce(_ + _)

    println(res)

    //    pi.map(i => println(s"pi array ${i}"))

    //    Thread.sleep(100000)
    blaze.stop()
    println("MPIPI has exited")
    System.exit(0)
  }
}
