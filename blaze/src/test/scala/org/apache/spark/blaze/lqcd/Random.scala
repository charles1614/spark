
// scalastyle:off println
package org.apache.spark.blaze.lqcd


object Random {

  var init, init2 = 0
  var twopi = 0d
  var pr, prm, ir, jr, is, is_old = 0
  var carry: Array[Int] = new Array[Int](4)
  var next = new Array[Int](96)

  // define constants
  val oneBit = 1.0 * math.pow(2, -24)

  for (k <- 0 until 96) {
    next(k) = (k + 1) % 96
    if (k % 4 == 3) {
      next(k) = (k + 5) % 96
    }
  }
  // union num in C version (init ranlux)
  val num = new Array[Int](96)
  // vec : 4*Int      doubleVec : 2*vec     vec : 12*doubleVec
  var vec: Array[Array[Array[Int]]] = Array.ofDim(12, 2, 4)

  /* return array of i element to fill RandomSU3Vector or SpinorField */
  def gauss(i: Int): Array[Double] = {
    //    val su3v = new SU3Vector
    var arr = new Array[Double](i)
    var ud = new Array[Double](2)

    if (init2 == 0) {
      twopi = 8.0 * math.atan(1.0)
      init = 1
    }

    for (k <- 0 until (i, 2) ) {
      ud = randlux(2)
      var x1 = ud(0)
      var x2 = ud(1)

      var rho = math.sqrt(-math.log(1.0d - x1))
      x2 *= twopi
      var y1 = rho * math.sin(x2)
      var y2 = rho * math.cos(x2)

      arr(k) = y1
      val l = k + 1
      arr(l) = y2

      //      su3v.set(k, y1)
      //      val m = k + 1
      //      su3v.set(m, y2)
    }

    //    su3v
    arr
  }

  def randlux(n: Int): Array[Double] = {
    var ret = new Array[Double](2)

    if (init == 0) {
      rlxInit(1, 1)
    }
    for (k <- 0 until n) {
      is = next(is)

      if (is == is_old) {
        update()
      }
      //      ret(k) = oneBit.toDouble * num(is + 4) + oneBit.toDouble * (num(is))
      // todo why plus 4
      ret(k) = oneBit * (vec(is / 8)(is % 8 / 4 + 1)(is % 8 % 4) +
        oneBit * vec(is / 8)(is % 8 / 4)(is % 8 % 4))
    }
    ret
  }

  def rlxInit(level: Int, seed: Int) {
    val xbit = new Array[Int](31)
    //    val ix,iy;

    //    error_loc((INT_MAX<2147483647)||(FLT_RADIX!=2)||(FLT_MANT_DIG<24)||
    //      (DBL_MANT_DIG<48),1,"rlxd_init [ranlxd.c]",
    //      "Arithmetic on this machine is not suitable for ranlxd");
    //
    //    define_constants();
    //
    //    error_loc((level<1)||(level>2),1,"rlxd_init [ranlxd.c]",
    //      "Bad choice of luxury level (should be 1 or 2)");

    if (level == 1) {
      pr = 202;
    }
    else if (level == 2) {
      pr = 397;
    }

    // var here
    var i = seed;

    for (k <- 0 until 31) {
      xbit(k) = i % 2;
      i /= 2;
    }

    //    error_loc((seed<=0)||(i!=0),1,"rlxd_init [ranlxd.c]",
    //      "Bad choice of seed (should be between 1 and 2^31-1)");

    // var here
    var ibit = 0
    var jbit = 18

    for (i <- 0 until 4) {
      for (k <- 0 until 24) {
        // var here
        var ix = 0

        for (l <- 0 until 24) {
          var iy = xbit(ibit)
          ix = 2 * ix + iy

          xbit(ibit) = (xbit(ibit) + xbit(jbit)) % 2
          ibit = (ibit + 1) % 31;
          jbit = (jbit + 1) % 31;
        }

        if ((k % 4) != i) {
          ix = 16777215 - ix
        }

        // c: union x here
        //        num(4 * k + i) = ix
        vec(k / 2)(k % 2)(i) = ix
      }
    }

    carry(0) = 0
    carry(1) = 0
    carry(2) = 0
    carry(3) = 0

    ir = 0
    jr = 7
    is = 91
    is_old = 0
    prm = pr % 12
    init = 1

  }

  def update(): Unit = {
    // double vector
    var pmin, pmax, pi, pj: Array[Array[Int]] = Array.ofDim(2, 4)

    var kmax = pr

    pmin = vecCopy(vec(0))
    pmax = vecCopy(vec(11))
    pi = vec(ir)
    pj = vec(jr)

    var ki, kj = 0

    // todo never run into
    for (k <- 0 until kmax) {
      var ret = step(pi, pj)
      pi = ret(0)
      pj = ret(1)
      ki += 1
      kj += 1
      if (ki == 11) {
        pi = pmin
        ki = 0
      }
      if (kj == 11) {
        pj = pmin
        kj = 0
      }
    }

    ir += prm
    jr += prm
    if (ir >= 12) {
      ir -= 12
    }
    if (jr >= 12) {
      jr -= 12
    }

    is = 8 * ir
    is_old = is
  }


  def step(pi: Array[Array[Int]], pj: Array[Array[Int]]): Array[Array[Array[Int]]] = {
    var ret: Array[Array[Array[Int]]] = Array.ofDim(2, 2, 4)
    var d = pj(0)(0) - pi(0)(0)
    val base = 0x1000000
    pi(1)(0) += (if (d < 0) 1 else 0)
    d += base
    ret(0) = pi
    ret(1) = pj
    ret
  }

  def vecCopy(v: Array[Array[Int]]): Array[Array[Int]] = {
    var vRet: Array[Array[Int]] = Array.ofDim(2, 4)
    for (i <- 0 until 2) {
      for (j <- 0 until 4)
        vRet(i)(j) = v(i)(j)
    }
    vRet
  }


}

