/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import org.apache.spark.storage.{BlockId, RDDBlockId}
import org.apache.spark.{Dependency, HashPartitioner, MPIDependency, Partition, Partitioner, ShuffleDependency, SparkEnv, TaskContext}

import scala.reflect.ClassTag

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 *
 * @param prev                  the parent RDD.
 * @param f                     The function used to map a tuple of (TaskContext, partition index, input iterator) to
 *                              an output iterator.
 * @param preservesPartitioning Whether the input function preserves the partitioner, which should
 *                              be `false` unless `prev` is a pair RDD and the input function
 *                              doesn't modify the keys.
 * @param isFromBarrier         Indicates whether this RDD is transformed from an RDDBarrier, a stage
 *                              containing at least one RDDBarrier shall be turned into a barrier stage.
 * @param isOrderSensitive      whether or not the function is order-sensitive. If it's order
 *                              sensitive, it may return totally different result when the input order
 *                              is changed. Mostly stateful functions are order-sensitive.
 */
private[spark] class MPIMapPartitionsRDD[U: ClassTag, T: ClassTag](
      var prev: RDD[T],
      f: (TaskContext, Int, Iterator[T]) => Iterator[U], // (TaskContext, partition index, iterator)
      preservesPartitioning: Boolean = false,
      isFromBarrier: Boolean = false,
      isOrderSensitive: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    val dep = dependencies.head.asInstanceOf[MPIDependency[T]]
    val metrics = context.taskMetrics().createTempShuffleReadMetrics()
    val itermid = SparkEnv.get.shuffleManager.getReader(
      dep.shuffleHandle, split.index, split.index + 1, context, metrics)
      .read()
      .asInstanceOf[Iterator[(Int, T)]]
    val iter = itermid.map(x => x._2)
//    iter.foreach(x => print(s"${split.index}: ${x} \n"))
    val ret = f(context, split.index, iter)
    ret
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  override def getDependencies: Seq[Dependency[_]] = {
//    println("prev Partitions is" + prev.getNumPartitions)
   Seq(new MPIDependency(prev, partitioner.getOrElse(new HashPartitioner(prev.getNumPartitions))))
  }

  @transient protected lazy override val isBarrier_ : Boolean =
    isFromBarrier || dependencies.exists(_.rdd.isBarrier())

  override protected def getOutputDeterministicLevel = {
    if (isOrderSensitive && prev.outputDeterministicLevel == DeterministicLevel.UNORDERED) {
      DeterministicLevel.INDETERMINATE
    } else {
      super.getOutputDeterministicLevel
    }
  }
}
