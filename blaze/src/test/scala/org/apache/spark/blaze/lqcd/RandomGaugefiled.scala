
// scalastyle:off println
package org.apache.spark.blaze.lqcd

object RandomGaugefiled {
  def main(args: Array[String]): Unit = {

    val t1 = System.nanoTime()


    // variables of iteration
    val kappa = 0.135
    val maxIter = 1000
    val tolerance = 1.0e-12
    val omega = 1.1
    printf("kappa= %f, omega= %f\n", kappa, omega)

    //    val su3Field = new SU3Field
    //    val input = new SpinorField
    //    val solution = new SpinorField

    val nseed = 10
    val istart = 1

    for (iseed <- istart until nseed) {
      printf("========================================\n");
      printf("Randomly initialization of gauge field\n and source vector for iseed=%d:\n\n", iseed);
      //      init_gauge_unit(=u);
      //      init_spinor_field_unit(=input);

//      val su3field = new SU3Field().initGaugeRandom(iseed)
//      val spinorField = new SpinorField().initSpinorRandom(iseed)
//
//      // init_guage_random and spinorfield pass
//      println("su3field first value is " + su3field.su3Field(0)(0)(0)(0)(0).v1.c1.re)
//      println("spinorfield first value is " + spinorField.spinorField(0)(0)(0)(0).v1.c1.re)
//
//      println("start slove linear system: =====================================")


      val su3field = new SU3Field().initGaugeRandom(iseed)
      val spinorField = new SpinorField().initSpinorRandom(iseed)
      val solution = new Dirac(su3field, spinorField).mrSolver()


      print("Inversion finished!\n");
      print("==================================\n");
      // multiply back the solution with Wilson-Dirac operator
      // to test the MR solver

      print("Test for the correctness of MR inverter:\n\n")
      //  spinor_field check;
      val check = new Dirac(su3field, spinorField).mWilson(solution)

      val res = math.sqrt((check -= spinorField).normSquareField())

      print("True residual is:\n")

      printf("|M=solution - source| = %-20.16e\n\n", res)

    }

    println("duration is " + (System.nanoTime() - t1)/1e9 + "s")
  }
}

