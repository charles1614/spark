
// scalastyle:off println
package org.apache.spark.blaze.lqcd

import java.io.{DataInputStream, File, FileInputStream}

object PTSource {

  def main(args: Array[String]): Unit = {
    val file = new File("examples/src/main/scala/org/apache/spark/examples/lqcd/PTSource.scala")
    val l = file.length()
    val in = new FileInputStream(file)
    val stream = new DataInputStream(in)
    val d = stream.readDouble()
    val u = new SU3Field()

    val field = ReadFileC.readGaugeFiledC("/home/xia/CLionProjects/simpleLQCD/main/test.8.cfg")
    println(field.su3Field(0)(0)(0)(0)(0).v1.c1.re)
    stream.close()
    in.close()
  }

}
