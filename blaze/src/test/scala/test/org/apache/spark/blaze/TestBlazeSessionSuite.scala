
package test.org.apache.spark.blaze

import org.apache.spark.SparkFunSuite
import org.apache.spark.blaze.BlazeSession


class TestBlazeSessionSuite extends SparkFunSuite {
  test("default seesion is set in constructor") {
    val blaze = BlazeSession
      .builder
      .appName("Test BlazeSession")
      .getOrCreate()

  }
}
