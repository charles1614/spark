
// scalastyle:off println
package org.apache.spark.blaze.lqcd

object Test {
  def u(u: SU3Field): Unit = {
    println("=====u : " + u.su3Field(0)(0)(0)(0)(0).v1.c1.re +
      "," + u.su3Field(0)(0)(0)(0)(0).v1.c1.im +
      u.su3Field(0)(0)(0)(7)(1).v3.c3.re +
      "," + u.su3Field(0)(0)(0)(7)(1).v3.c3.im)
  }

  def sp(sp: SpinorField): Unit = {
    println("=====sp : " + sp.spinorField(0)(0)(0)(0).v1.c1.re + "," +
      sp.spinorField(0)(0)(0)(0).v1.c1.im + "\t" +
      sp.spinorField(0)(0)(0)(7).v3.c3.re + "," +
      sp.spinorField(0)(0)(0)(7).v3.c3.im)
  }

  def spr(spr: Spinor): Unit = {
    println("=====sp : " + spr.v1.c1.re + "," +
      spr.v1.c1.im + "\t" +
      spr.v3.c3.re + "," +
      spr.v3.c3.im)
  }

}
