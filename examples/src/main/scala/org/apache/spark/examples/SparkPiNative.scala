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

//import scala.math.random
import org.apache.spark.BlazeSession

/** Computes an approximation to pi */
object SparkPiNative {

  def main(args: Array[String]): Unit = {

    val blaze = BlazeSession
      .builder
      .appName("blazePi")
      .master("local[1]")
      .getOrCreate()

    val start = System.nanoTime()
    val slices = if (args.length > 0) args(0).toInt else 8
//    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
//    val count = blaze.blazeContext.parallelize(1 until n, slices).map { i =>
//      val x = random * 2 - 1
//      val y = random * 2 - 1
//      if (x * x + y * y <= 1) 1 else 0
//    }.reduce(_ + _)

    val nativeExec = "hostname";

    val n = 8
    val count = blaze.blazeContext.parallelize(1 until n, slices).map { i =>
      exec(nativeExec)
    }.count()

    //    blaze.sparkContext.parallelize(1 until 3, slices).map(i => mpiop(args)).collect()
    //    blaze.mpiContext.parallelize(1 until 2, slices).map(i => mpiop(args)).collect()
    //    mpiop(args);
    val end = System.nanoTime()
//    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
//    println(s"elapse time is ${(end - start) / 1000000} ms")
    blaze.stop()
  }

  def exec(exe: String): Unit = {
    val pb = new ProcessBuilder(exe).inheritIO()
    val p = pb.start()
    p.waitFor()
//    pb.wait()
  }
}
// scalastyle:on println
