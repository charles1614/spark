
package org.apache.spark.blaze.ompi

class MPIJobInfo {

  def this(ns: String) {
    this()
    namespace = ns
  }

  private var namespace: String = _
}
