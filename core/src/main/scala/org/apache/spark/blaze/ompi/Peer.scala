
package org.apache.spark.blaze.ompi

class Peer {
  private var namespace: String = _
  private var rank: Int = _
  def this(ns: String, r: Int) = {
    this()
    namespace = ns
    rank = r
  }
}
