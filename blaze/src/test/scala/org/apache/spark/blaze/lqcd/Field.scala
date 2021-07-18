
// scalastyle:off println
package org.apache.spark.blaze.lqcd

class SU3Field() {
  var nt, nz, ny, nx = 8
  var mu = 4
  var iseed = 0
  var su3Field: Array[Array[Array[Array[Array[SU3]]]]] = Array.ofDim(nt, nz, ny, nx, mu)
  val epsilon = math.ulp(1.0f)


  def this(iseed: Int) {
    this()
    this.iseed = iseed
    initGaugeRandom(iseed)
  }

  def this(nt: Int, nz: Int, ny: Int, nx: Int, mu: Int, iseed: Int) {
    this(iseed)
    this.nt = nt
    this.nz = nz
    this.ny = ny
    this.nx = nx
    this.mu = mu
  }


  def initGaugeRandom(iseed: Int): SU3Field = {
    for (it <- 0 until nt;
         iz <- 0 until nz;
         iy <- 0 until ny;
         ix <- 0 until nx;
         imu <- 0 until mu) {
      val index = it * nz * ny * nx * mu + iz * ny * nx * mu + iy * nx * mu + ix * mu + imu
      Random.rlxInit(0, iseed + index)
      this.su3Field(it)(iz)(iy)(ix)(imu) = randomSU3()
    }
    this
  }

  def readGaugeField(d: Double): Int = {
    this.su3Field(0)(0)(0)(0)(0).v1.c1.re = d
    0
  }


  def randomSU3(): SU3 = {
    var su3 = new SU3

    su3.v1 = randomSU3vector()

    var vNorm: Double = 0d

    while (vNorm <= epsilon) {
      su3.v2 = randomSU3vector()
      su3.v3 = su3.v1.cross(su3.v2)
      vNorm = math.sqrt(su3.v3.norm)
    }

    val factor = 1.0 / vNorm
    su3.v3 = su3.v3.multiply(factor)
    su3.v2 = su3.v3.cross(su3.v1)
    su3
  }

  def randomSU3vector(): SU3Vector = {
    var su3v = new SU3Vector
    var vNorm: Double = 0f
    // todo why flt_epsilon not dle_spsilon
    while (vNorm <= epsilon) {
      val doubles = Random.gauss(6)
      su3v = su3v.copyFromArray(doubles)
      vNorm = su3v.norm
      vNorm = math.sqrt(vNorm)
    }

    var factor = 1.0f / vNorm
    su3v.multiply(factor)
  }

  /*
  v.c1=(w.c2*z.c3-w.c3*z.c2)
  v.c2=(w.c3*z.c1-w.c1*z.c3)
  v.c3=(w.c1*z.c2-w.c2*z.c1)
  */
  //  def cross(v1: SU3Vector, v2: SU3Vector): SU3Vector = {
  //    v.c1.re = v1.c2.re * v2.c3.re - v1.c2.im * v2.c3.im - v1.c3.re * v2.c2.re + v1.c3.im * v2.c2.im
  //    v.c1.im = v1.c3.re * v2.c2.im + v1.c3.im * v2.c2.re - v1.c2.re * v2.c3.im - v1.c2.im * v2.c3.re
  //    v.c2.re = v1.c3.re * v2.c1.re - v1.c3.im * v2.c1.im - v1.c1.re * v2.c3.re + v1.c1.im * v2.c3.im
  //    v.c2.im = v1.c1.re * v2.c3.im + v1.c1.im * v2.c3.re - v1.c3.re * v2.c1.im - v1.c3.im * v2.c1.re
  //    v.c3.re = v1.c1.re * v2.c2.re - v1.c1.im * v2.c2.im - v1.c2.re * v2.c1.re + v1.c2.im * v2.c1.im
  //    v.c3.im = v1.c2.re * v2.c1.im + v1.c2.im * v2.c1.re - v1.c1.re * v2.c2.im - v1.c1.im * v2.c2.re
  //  }

}

final case class SpinorField() {
  val nt, nz, ny, nx = 8
  val spinorField: Array[Array[Array[Array[Spinor]]]] = Array.ofDim(nt, nz, ny, nx)
  // random_spinor sigma
  var sigma = 1.0

  //  override def clone(): AnyRef = SpinorField.super.clone()

  def initZero(): SpinorField = {
    for (it <- 0 until nt;
         iz <- 0 until nz;
         iy <- 0 until ny;
         ix <- 0 until nx) {
      //            this.spinorField(it)(iz)(iy)(ix) = new Spinor().initZero()
      this.spinorField(it)(iz)(iy)(ix) = new Spinor()
    }
    this
  }

  def initSpinorRandom(iseed: Int): SpinorField = {
    for (it <- 0 until nt;
         iz <- 0 until nz;
         iy <- 0 until ny;
         ix <- 0 until nx) {
      var index = it * nz * ny * nx + iz * ny * nx + iy * nx + ix
      Random.rlxInit(0, iseed + index)
      this.spinorField(it)(iz)(iy)(ix) = randomSpinor(sigma)
    }
    this
  }

  def randomSpinor(d: Double): Spinor = {
    val spinor = new Spinor
    val vector = Random.gauss(24)
    spinor.copyFromArray(vector)
    spinor
  }

  // spinorfield vdot spinorfield
  def vdot(sf: SpinorField): Complex = {
    var real, imag = 0d
    var complex = new Complex
    for (it <- 0 until nt; iz <- 0 until nz; iy <- 0 until ny; ix <- 0 until nx) {
      complex += this.spinorField(it)(iz)(iy)(ix).vdot(sf.spinorField(it)(iz)(iy)(ix))
    }
    complex
  }

  // complex multiplication
  def mult(cpl: Complex): SpinorField = {
    val ret = new SpinorField
    for (it <- 0 until nt; iz <- 0 until nz; iy <- 0 until ny; ix <- 0 until nx) {
      ret.spinorField(it)(iz)(iy)(ix) = this.spinorField(it)(iz)(iy)(ix).multiply(cpl)
    }
    ret
  }

  // real multiplication
  def mult(real: Double): SpinorField = {
    val ret = new SpinorField
    for (it <- 0 until nt; iz <- 0 until nz; iy <- 0 until ny; ix <- 0 until nx) {
      ret.spinorField(it)(iz)(iy)(ix) = this.spinorField(it)(iz)(iy)(ix).mult(real)
    }
    ret
  }

  def normSquareField(): Double = {
    var double: Double = 0d
    for (it <- 0 until nt; iz <- 0 until nz; iy <- 0 until ny; ix <- 0 until nx) {
      double += this.spinorField(it)(iz)(iy)(ix).normSquare()
    }
    double
  }

  def add(sf: SpinorField): SpinorField = {
    val ret = new SpinorField
    for (it <- 0 until nt; iz <- 0 until nz; iy <- 0 until ny; ix <- 0 until nx) {
      ret.spinorField(it)(iz)(iy)(ix) = this.spinorField(it)(iz)(iy)(ix) +
        sf.spinorField(it)(iz)(iy)(ix)
    }
    ret
  }

  def sub(sf: SpinorField): SpinorField = {
    val ret = new SpinorField
    for (it <- 0 until nt; iz <- 0 until nz; iy <- 0 until ny; ix <- 0 until nx) {
      ret.spinorField(it)(iz)(iy)(ix) = this.spinorField(it)(iz)(iy)(ix) -
        sf.spinorField(it)(iz)(iy)(ix)
    }
    ret
  }

  def -=(sf: SpinorField): SpinorField = {
    val ret = new SpinorField
    for (it <- 0 until nt; iz <- 0 until nz; iy <- 0 until ny; ix <- 0 until nx) {
      this.spinorField(it)(iz)(iy)(ix) = this.spinorField(it)(iz)(iy)(ix) -
        sf.spinorField(it)(iz)(iy)(ix)
    }
    this
  }


  def +=(sf: SpinorField): SpinorField = {
    val ret = new SpinorField
    for (it <- 0 until nt; iz <- 0 until nz; iy <- 0 until ny; ix <- 0 until nx) {
      this.spinorField(it)(iz)(iy)(ix) = this.spinorField(it)(iz)(iy)(ix) +
        sf.spinorField(it)(iz)(iy)(ix)
    }
    this
  }


}


final class SU3Vector {
  var c1, c2, c3: Complex = new Complex


  def this(c1: Complex, c2: Complex, c3: Complex) = {
    this()
    this.c1 = c1
    this.c2 = c2
    this.c3 = c3
  }

  def norm: Double = {
    c1.norm + c2.norm + c3.norm
  }

  def multiply(factor: Double): SU3Vector = {
    new SU3Vector(c1.fact(factor), c2.fact(factor), c3.fact(factor))
  }

  // vector vdot vector
  def vdot(s: SU3Vector): Complex = {
    c1.vdot(s.c1) + c2.vdot(s.c2) + c3.vdot(s.c3)
  }

  def multiply(cpl: Complex): SU3Vector = {
    new SU3Vector(c1 * cpl, c2 * cpl, c3 * cpl)
  }

  // vector dot matrix
  def dot(u: SU3): SU3Vector = {
    var uDagger = u.dagger()
    new SU3Vector(this.dot(uDagger.v1), this.dot(uDagger.v2), this.dot(uDagger.v1))
    //    c1.re = u.v1.c1.re * this.c1.re + u.v1.c1.im * this.c1.im +
    //      u.v2.c1.re * this.c2.re + u.v2.c1.im * this.c2.im +
    //      u.v3.c1.re * this.c3.re + u.v3.c1.im * this.c3.im
    //    c1.im = u.v1.c1.re * this.c1.im - u.v1.c1.im * this.c1.re +
    //      u.v2.c1.re * this.c2.im - u.v2.c1.im * this.c2.re +
    //      u.v3.c1.re * this.c3.im - u.v3.c1.im * this.c3.re
    //
    //    c2.re = u.v1.c2.re * this.c1.re + u.v1.c2.im * this.c1.im +
    //      u.v2.c2.re * this.c2.re + u.v2.c2.im * this.c2.im +
    //      u.v3.c2.re * this.c3.re + u.v3.c2.im * this.c3.im
    //    c2.im = u.v1.c2.re * this.c1.im - u.v1.c2.im * this.c1.re +
    //      u.v2.c2.re * this.c2.im - u.v2.c2.im * this.c2.re +
    //      u.v3.c2.re * this.c3.im - u.v3.c2.im * this.c3.re
    //
    //    c3.re = u.v1.c3.re * this.c1.re + u.v1.c3.im * this.c1.im +
    //      u.v2.c3.re * this.c2.re + u.v2.c3.im * this.c2.im +
    //      u.v3.c3.re * this.c3.re + u.v3.c3.im * this.c3.im
    //    c3.im = u.v1.c3.re * this.c1.im - u.v1.c3.im * this.c1.re +
    //      u.v2.c3.re * this.c2.im - u.v2.c3.im * this.c2.re +
    //      u.v3.c3.re * this.c3.im - u.v3.c3.im * this.c3.re
  }

  def dot(su: SU3Vector): Complex = {
    c1 * su.c1 + c2 * su.c2 + c3 * su.c3
  }


  /*
  v.c1=(w.c2*z.c3-w.c3*z.c2)
  v.c2=(w.c3*z.c1-w.c1*z.c3)
  v.c3=(w.c1*z.c2-w.c2*z.c1)
  */
  def cross(v: SU3Vector): SU3Vector = {
    new SU3Vector((this.c2 * v.c3 - v.c2 * this.c3).conjugate(),
      (this.c3 * v.c1 - this.c1 * v.c3).conjugate(),
      (this.c1 * v.c2 - this.c2 * v.c1).conjugate())
  }

  def +(su: SU3Vector): SU3Vector = {
    new SU3Vector(this.c1 + su.c1, this.c2 + su.c2, this.c3 + su.c3)
  }

  def -(su: SU3Vector): SU3Vector = {
    new SU3Vector(this.c1 - su.c1, this.c2 - su.c2, this.c3 - su.c3)
  }

  // ret = this - i* su
  def iSub(su: SU3Vector): SU3Vector = {
    val complex = new Complex(0, 1)
    new SU3Vector(this.c1 - su.c1.multiply(complex), this.c2 - su.c2, this.c3 - su.c3)
  }

  def iAdd(su: SU3Vector): SU3Vector = {
    c1 = this.c1.iAdd(su.c1)
    c2 = this.c2.iAdd(su.c2)
    c3 = this.c3.iAdd(su.c3)
    this
  }

  def iMul(): SU3Vector = {
    c1 = this.c1.iMul()
    c2 = this.c2.iMul()
    c3 = this.c3.iMul()
    this
  }

  def miMul(): SU3Vector = {
    c1 = this.c1.miMul()
    c2 = this.c2.miMul()
    c3 = this.c3.miMul()
    this
  }


  def set(n: Int, v: Double): SU3Vector = {
    val c = n / 2
    c match {
      case 0 => c1.set(n % 2, v)
      case 1 => c2.set(n % 2, v)
      case 2 => c3.set(n % 2, v)
    }
    this
  }


  def copyFromArray(array: Array[Double]): SU3Vector = {
    for (k <- 0 until(6, 2)) {
      this.set(k, array(k))
      val m = k + 1
      this.set(m, array(m))
    }
    this
  }
}

// vector(1*3) of SU3Vector
// matrix(3*3) of Complex
// store by row
final class SU3 {
  var v1, v2, v3: SU3Vector = new SU3Vector

  def dagger(): SU3 = {
    val su = new SU3
    su.v1.c1 = v1.c1.conjugate()
    su.v1.c2 = v2.c1.conjugate()
    su.v1.c3 = v3.c1.conjugate()
    su.v2.c1 = v1.c2.conjugate()
    su.v2.c2 = v2.c2.conjugate()
    su.v2.c3 = v3.c2.conjugate()
    su.v3.c1 = v1.c3.conjugate()
    su.v3.c2 = v2.c3.conjugate()
    su.v3.c3 = v3.c3.conjugate()
    su
  }

  def matmul(sp: Spinor): Spinor = {
    new Spinor(this.matmul(sp.v1), this.matmul(sp.v2), this.matmul(sp.v3), this.matmul(sp.v4))
  }

  def matmul(v: SU3Vector): SU3Vector = {
    new SU3Vector(this.v1.dot(v), this.v2.dot(v), this.v3.dot(v))
  }
}


// vector(1*4) of SU3Vector
// matrix(4*3) of Complex3
final class Spinor() {
  var v1, v2, v3, v4: SU3Vector = new SU3Vector

  def this(v1: SU3Vector, v2: SU3Vector, v3: SU3Vector, v4: SU3Vector) {
    this()
    this.v1 = v1
    this.v2 = v2
    this.v3 = v3
    this.v4 = v4
  }


  def initZero(): Spinor = {
    for (i <- 0 to 7) {
      set(i, 0d)
    }
    this
  }

  def set(n: Int, v: Double): Spinor = {
    val c = n / 2
    c match {
      case 0 => this.v1.set(n % 2, v)
      case 1 => this.v2.set(n % 2, v)
      case 2 => this.v3.set(n % 2, v)
      case 3 => this.v4.set(n % 2, v)
    }
    this
  }

  def copyFromArray(array: Array[Double]): Spinor = {
    for (k <- 0 until(24, 6)) {
      val tmp = array.slice(k, k + 6)
      val c = k / 6
      c match {
        case 0 => this.v1.copyFromArray(tmp)
        case 1 => this.v2.copyFromArray(tmp)
        case 2 => this.v3.copyFromArray(tmp)
        case 3 => this.v4.copyFromArray(tmp)
      }
    }
    this
  }

  // spinor vdot spinor
  def vdot(sp: Spinor): Complex = {
    v1.vdot(sp.v1) +
      v2.vdot(sp.v2) +
      v3.vdot(sp.v3) +
      v4.vdot(sp.v4)
  }


  // spinor multiply complex
  def multiply(cpl: Complex): Spinor = {
    new Spinor(v1.multiply(cpl), v2.multiply(cpl), v3.multiply(cpl), v4.multiply(cpl))
  }

  // spinor matrix multiply SU3 matrix
  def matmul(u: SU3): Spinor = {
    new Spinor(v1.dot(u), v2.dot(u), v3.dot(u), v4.dot(u))
  }

  // spinor multiply real
  def mult(real: Double): Spinor = {
    new Spinor(v1.multiply(real), v2.multiply(real), v3.multiply(real), v4.multiply(real))
  }


  def add(sp: Spinor): Spinor = {
    new Spinor(this.v1 + sp.v1, this.v2 + sp.v2, this.v3 + sp.v3, this.v4 + sp.v4)
  }

  def +(sp: Spinor): Spinor = {
    add(sp)
  }

  def +=(sp: Spinor): Spinor = {
    this.v1 = this.v1 + sp.v1
    this.v2 = this.v2 + sp.v2
    this.v3 = this.v3 + sp.v3
    this.v4 = this.v4 + sp.v4
    this
  }

  def -(sp: Spinor): Spinor = {
    new Spinor(this.v1 - sp.v1, this.v2 - sp.v2, this.v3 - sp.v3, this.v4 - sp.v4)
  }

  def normSquare(): Double = {
    (v1.vdot(v1) + v2.vdot(v2) + v3.vdot(v3) + v4.vdot(v4)).re
  }

  // Wilson-Dirac
  def onePmGammaMult(mu: Int, pm: Int): Spinor = {
    val ret = new Spinor()
    if (pm == -1) {
      mu match {
        // (1-gamma_0)* spinor
        case 0 =>
          ret.v1 = this.v1 - this.v4.multiply(new Complex(0, 1))
          ret.v2 = this.v2 - this.v3.multiply(new Complex(0, 1))
          ret.v3 = ret.v2.multiply(new Complex(0, 1))
          ret.v4 = ret.v1.multiply(new Complex(0, 1))

        // (1-gamma_1)* spinor
        case 1 =>
          ret.v1 = this.v1 + this.v4
          ret.v2 = this.v2 - this.v3
          ret.v3 = ret.v2.multiply(-1.0)
          ret.v4 = ret.v1

        // (1-gamma_2)* spinor
        case 2 =>
          ret.v1 = this.v1 - this.v3.multiply(new Complex(0, 1))
          ret.v2 = this.v2 + this.v4.multiply(new Complex(0, 1))
          ret.v3 = ret.v1.multiply(new Complex(0, 1))
          ret.v4 = ret.v2.multiply(new Complex(0, -1))

        // (1-gamma_3)* spinor
        case 3 =>
          ret.v1 = this.v1 - this.v3
          ret.v2 = this.v2 - this.v4
          ret.v3 = ret.v1.multiply(-1.0)
          ret.v4 = ret.v2.multiply(-1.0)

        case _ => throw new Exception("wrong parameter pm!")

      }
    } else if (pm == 1) {
      mu match {
        // (1+gamma_0)* spinor
        case 0 =>
          ret.v1 = this.v1 + this.v4.multiply(new Complex(0, 1))
          ret.v2 = this.v2 + this.v3.multiply(new Complex(0, 1))
          ret.v3 = ret.v2.multiply(new Complex(0, -1))
          ret.v4 = ret.v1.multiply(new Complex(0, -1))

        // (1+gamma_1)* spinor
        case 1 =>
          ret.v1 = this.v1 - this.v4
          ret.v2 = this.v2 + this.v3
          ret.v3 = ret.v2
          ret.v4 = ret.v1.multiply(-1.0)

        // (1+gamma_2)* spinor
        case 2 =>
          ret.v1 = this.v1 + this.v3.multiply(new Complex(0, 1))
          ret.v2 = this.v2 - this.v4.multiply(new Complex(0, 1))
          ret.v3 = ret.v1.multiply(new Complex(0, -1))
          ret.v4 = ret.v2.multiply(new Complex(0, 1))

        // (1+gamma_3)* spinor
        case 3 =>
          ret.v1 = this.v1 + this.v3
          ret.v2 = this.v2 + this.v4
          ret.v3 = ret.v1
          ret.v4 = ret.v2

        case _ => throw new Exception("wrong parameter pm!")
      }
    }
    ret
  }

}

final class Complex {

  var re, im: Double = 0d

  def this(re: Double, im: Double) {
    this()
    this.re = re
    this.im = im
  }

  def norm: Double = {
    re * re + im * im
  }

  def fact(factor: Double): Complex = {
    new Complex(this.re * factor, this.im * factor)
  }

  //  def multiply(cpl: Complex): Complex = {
  //    new Complex(this.re * cpl.re, this.im * cpl.im)
  //  }

  def multiply(cpl: Complex): Complex = {
    new Complex(this.re * cpl.re - this.im * cpl.im, this.re * cpl.im + this.im * cpl.re)
  }


  def *(cpl: Complex): Complex = {
    multiply(cpl)
  }

  def mult(real: Double): Complex = {
    new Complex(this.re * real, this.im * real)
  }

  // r = <s1,s2> = s1^* * s2
  // r.re = s1.re*s2.re + s1.im*s2.im
  // r.im = s1.re*s2.im - s1.im*s2.re
  def vdot(cpl: Complex): Complex = {
    new Complex(this.re * cpl.re + this.im * cpl.im, this.re * cpl.im - this.im * cpl.re)
  }

  def +(cpl: Complex): Complex = {
    new Complex(this.re + cpl.re, this.im + cpl.im)
  }

  def +=(cpl: Complex): Complex = {
    this.re = (this + cpl).re
    this.im = (this + cpl).im
    this
  }

  def -(cpl: Complex): Complex = {
    new Complex(this.re - cpl.re, this.im - cpl.im)
  }

  def iSub(cpl: Complex): Complex = {
    new Complex(this.re + cpl.im, this.im - cpl.re)
  }

  def iAdd(cpl: Complex): Complex = {
    new Complex(this.re - cpl.im, this.im + cpl.re)
  }

  def iMul(): Complex = {
    new Complex(-(this.im), this.re)
  }

  def miMul(): Complex = {
    new Complex(this.im, -(this.re))
  }

  def conjugate(): Complex = {
    new Complex(this.re, -this.im)
  }

  // set re and im
  def set(n: Int, v: Double): Complex = {
    n match {
      case 0 => this.re = v
      case 1 => this.im = v
    }
    this
  }


}




