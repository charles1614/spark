
package org.apache.spark.examples.blaze.linalg

import org.apache.spark.Partitioner

class NumPartitioner(val partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    (key.asInstanceOf[Long] % partitions).toInt
  }
}
