

// scalastyle:off println
package org.apache.spark.blaze.lqcd

final class Dirac {

  val maxIterator = 1000
  val tolerance = 1.0e-12
  val omega = 1.1
  val kappa = 0.135
  var nt, nz, ny, nx = 8
  var count = 0
  private var u = new SU3Field
  private var sf = new SpinorField

  def this(u: SU3Field, sf: SpinorField) {
    this()
    this.u = u
    this.sf = sf
  }

  def initSpinorFieldZero(solution: SpinorField): SpinorField = {
    var ret = new SpinorField
    this.sf
  }

  def mWilson(sf: SpinorField): SpinorField = {
    val c1 = 1d / (2 * kappa)
    val c2 = 0.5d
    val ret = sf.mult(c1)
    val tmp1 = dslash(sf)
    //    Test.sp(tmp1)
    ret -= tmp1.mult(c2)
  }

  // input: spinorfiled    u: su3field
  def dslash(sf: SpinorField): SpinorField = {
    var ret = new SpinorField
    for (it <- 0 until nt;
         iz <- 0 until nz;
         iy <- 0 until ny;
         ix <- 0 until nx) {

      var sp = new Spinor().initZero()

      // x-direction, mu=0
      // forword
      sp += u.su3Field(it)(iz)(iy)(ix)(0)
        .matmul(sf.spinorField(it)(iz)(iy)((ix + 1) % nx))
        .onePmGammaMult(0, -1)

      // backward
      sp += u.su3Field(it)(iz)(iy)((ix + nx - 1) % nx)(0).dagger()
        .matmul(sf.spinorField(it)(iz)(iy)((ix + nx - 1) % nx))
        .onePmGammaMult(0, 1)



      // y-direction, mu=1
      sp += u.su3Field(it)(iz)(iy)(ix)(1)
        .matmul(sf.spinorField(it)(iz)((iy + 1) % ny)(ix))
        .onePmGammaMult(1, -1)


      // backward
      sp += u.su3Field(it)(iz)((iy + ny - 1) % ny)(ix)(1).dagger()
        .matmul(sf.spinorField(it)(iz)((iy + ny - 1) % ny)(ix))
        .onePmGammaMult(1, 1)



      // z-direction, mu+=1
      sp += u.su3Field(it)(iz)(iy)(ix)(2)
        .matmul(sf.spinorField(it)((iz + 1) % nz)(iy)(ix))
        .onePmGammaMult(2, -1)


      // backward
      sp += u.su3Field(it)((iz + nz - 1) % nz)(iy)(ix)(2).dagger()
        .matmul(sf.spinorField(it)((iz + nz - 1) % nz)(iy)(ix))
        .onePmGammaMult(2, 1)


      // t-direction, mu+=1
      sp += u.su3Field(it)(iz)(iy)(ix)(3)
        .matmul(sf.spinorField((it + 1) % nt)(iz)(iy)(ix))
        .onePmGammaMult(3, -1)


      // backward
      sp += u.su3Field((it + nt - 1) % nt)(iz)(iy)(ix)(3).dagger()
        .matmul(sf.spinorField((it + nt - 1) % nt)(iz)(iy)(ix))
        .onePmGammaMult(3, 1)


      ret.spinorField(it)(iz)(iy)(ix) = sp
    }
    ret
  }

  def mrSolver(): SpinorField = {

    // todo initZero (not needed?)
    var solution = new SpinorField().initZero()

    var tmp, r, Mr = new SpinorField

    tmp = mWilson(solution)


    // todo
    r = sf.sub(tmp)

    var residual = math.sqrt(r.normSquareField())


    var coef1, coef2: Complex = new Complex
    var coef: Double = 0d

    while (count < maxIterator && residual > tolerance) {
      count = count + 1

      Mr = mWilson(r)


      coef1 = Mr.vdot(r)
      coef = 1d / Mr.normSquareField() * omega
      // multiplication of real and complex number
      var coef2 = coef1.fact(coef)

      // multiplication of complex and spinor field
      tmp = r.mult(coef2)
      solution += tmp
      tmp = Mr.mult(coef2)
      r -= tmp

      residual = math.sqrt(r.normSquareField())

      print("iteration " + count + " redidual " + residual + "\n")


    }

    solution
  }
}
