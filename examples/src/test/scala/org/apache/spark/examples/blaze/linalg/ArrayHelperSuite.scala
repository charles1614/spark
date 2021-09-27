
package org.apache.spark.examples.blaze.linalg

import org.apache.spark.examples.blaze.linalg.ArrayHelper._
import org.junit.Assert._

import java.util

class ArrayHelperSuite {

  @org.junit.Test
  def testMult(): Unit = {
    val array: Array[Double] = Array(1, 2, 3, 4, 5)
    assertTrue(util.Arrays.equals(array * 2, Array[Double](2, 4, 6, 8, 10)))
  }

  @org.junit.Test
  def testDot(): Unit = {
    val array: Array[Double] = Array(1, 2, 3, 4, 5)
    assertEquals(array dot array, 55, 0.001)
  }

  @org.junit.Test
  def testPlus(): Unit = {
    val array: Array[Double] = Array(1, 2, 3, 4, 5)
    assertTrue(util.Arrays.equals(array + array, array * 2))
  }

  @org.junit.Test
  def testSub(): Unit = {
    val array: Array[Double] = Array(1, 2, 3, 4, 5)
    assertTrue(util.Arrays.equals(array - array, Array.fill(5)(0.toDouble)))
  }
}
