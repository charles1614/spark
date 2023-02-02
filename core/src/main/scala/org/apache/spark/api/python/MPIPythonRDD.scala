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

package org.apache.spark.api.python

import java.io._
import java.net._
import java.nio.charset.StandardCharsets
import java.util.{ArrayList => JArrayList}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{InputFormat, JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, OutputFormat => NewOutputFormat}
import org.apache.spark._
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.input.PortableDataStream
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD}
import org.apache.spark.security.{SocketAuthHelper, SocketAuthServer, SocketFuncServer}
import org.apache.spark.util._

private[spark] class MPIPythonRDD(
  parent: RDD[_],
  func: PythonFunction,
  preservePartitioning: Boolean,
  isFromBarrier: Boolean = false)
  extends PythonRDD(parent, func, preservePartitioning, isFromBarrier) {
  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {

    // MPI
    //    dependencies.head.asInstanceOf[MPIDependency[_]]

    // TODO: Force Skip Exception
    try {
      dependencies.head.asInstanceOf[MPIDependency[_]]
    } catch {
      case e: Exception => println("MPIPythonRDD: " + e)
    }

    val runner = PythonRunner(func)
    runner.compute(firstParent.iterator(split, context), split.index, context)
  }

  @transient protected lazy override val isBarrier_ : Boolean =
    true
}

private[spark] object MPIPythonRDD extends Logging {

  // remember the broadcasts sent to each worker
  private val workerBroadcasts = new mutable.WeakHashMap[Socket, mutable.Set[Long]]()

  // Authentication helper used when serving iterator data.
  private lazy val authHelper = {
    val conf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new SocketAuthHelper(conf)
  }

  def getWorkerBroadcasts(worker: Socket): mutable.Set[Long] = {
    synchronized {
      workerBroadcasts.getOrElseUpdate(worker, new mutable.HashSet[Long]())
    }
  }

  /**
   * Return an RDD of values from an RDD of (Long, Array[Byte]), with preservePartitions=true
   *
   * This is useful for PySpark to have the partitioner after partitionBy()
   */
  def valueOfPair(pair: JavaPairRDD[Long, Array[Byte]]): JavaRDD[Array[Byte]] = {
    pair.rdd.mapPartitions(it => it.map(_._2), true)
  }

  /**
   * Adapter for calling SparkContext#runJob from Python.
   *
   * This method will serve an iterator of an array that contains all elements in the RDD
   * (effectively a collect()), but allows you to run on a certain subset of partitions,
   * or to enable local execution.
   *
   * @return 3-tuple (as a Java array) with the port number of a local socket which serves the
   *         data collected from this job, the secret for authentication, and a socket auth
   *         server object that can be used to join the JVM serving thread in Python.
   */
  def runJob(
    sc: SparkContext,
    rdd: JavaRDD[Array[Byte]],
    partitions: JArrayList[Int]): Array[Any] = {
    type ByteArray = Array[Byte]
    type UnrolledPartition = Array[ByteArray]
    val allPartitions: Array[UnrolledPartition] =
      sc.runJob(rdd, (x: Iterator[ByteArray]) => x.toArray, partitions.asScala)
    val flattenedPartition: UnrolledPartition = Array.concat(allPartitions: _*)
    serveIterator(flattenedPartition.iterator,
      s"serve RDD ${rdd.id} with partitions ${partitions.asScala.mkString(",")}")
  }

  /**
   * A helper function to collect an RDD as an iterator, then serve it via socket.
   *
   * @return 3-tuple (as a Java array) with the port number of a local socket which serves the
   *         data collected from this job, the secret for authentication, and a socket auth
   *         server object that can be used to join the JVM serving thread in Python.
   */
  def collectAndServe[T](rdd: RDD[T]): Array[Any] = {
    serveIterator(rdd.collect().iterator, s"serve RDD ${rdd.id}")
  }

  /**
   * A helper function to collect an RDD as an iterator, then serve it via socket.
   * This method is similar with `PythonRDD.collectAndServe`, but user can specify job group id,
   * job description, and interruptOnCancel option.
   */
  def collectAndServeWithJobGroup[T](
    rdd: RDD[T],
    groupId: String,
    description: String,
    interruptOnCancel: Boolean): Array[Any] = {
    val sc = rdd.sparkContext
    sc.setJobGroup(groupId, description, interruptOnCancel)
    serveIterator(rdd.collect().iterator, s"serve RDD ${rdd.id}")
  }

  /**
   * A helper function to create a local RDD iterator and serve it via socket. Partitions are
   * are collected as separate jobs, by order of index. Partition data is first requested by a
   * non-zero integer to start a collection job. The response is prefaced by an integer with 1
   * meaning partition data will be served, 0 meaning the local iterator has been consumed,
   * and -1 meaning an error occurred during collection. This function is used by
   * pyspark.rdd._local_iterator_from_socket().
   *
   * @return 3-tuple (as a Java array) with the port number of a local socket which serves the
   *         data collected from this job, the secret for authentication, and a socket auth
   *         server object that can be used to join the JVM serving thread in Python.
   */
  def toLocalIteratorAndServe[T](rdd: RDD[T], prefetchPartitions: Boolean = false): Array[Any] = {
    val handleFunc = (sock: Socket) => {
      val out = new DataOutputStream(sock.getOutputStream)
      val in = new DataInputStream(sock.getInputStream)
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // Collects a partition on each iteration
        val collectPartitionIter = rdd.partitions.indices.iterator.map { i =>
          var result: Array[Any] = null
          rdd.sparkContext.submitJob(
            rdd,
            (iter: Iterator[Any]) => iter.toArray,
            Seq(i), // The partition we are evaluating
            (_, res: Array[Any]) => result = res,
            result)
        }
        val prefetchIter = collectPartitionIter.buffered

        // Write data until iteration is complete, client stops iteration, or error occurs
        var complete = false
        while (!complete) {

          // Read request for data, value of zero will stop iteration or non-zero to continue
          if (in.readInt() == 0) {
            complete = true
          } else if (prefetchIter.hasNext) {

            // Client requested more data, attempt to collect the next partition
            val partitionFuture = prefetchIter.next()
            // Cause the next job to be submitted if prefetchPartitions is enabled.
            if (prefetchPartitions) {
              prefetchIter.headOption
            }
            val partitionArray = ThreadUtils.awaitResult(partitionFuture, Duration.Inf)

            // Send response there is a partition to read
            out.writeInt(1)

            // Write the next object and signal end of data for this iteration
            writeIteratorToStream(partitionArray.toIterator, out)
            out.writeInt(SpecialLengths.END_OF_DATA_SECTION)
            out.flush()
          } else {
            // Send response there are no more partitions to read and close
            out.writeInt(0)
            complete = true
          }
        }
      })(catchBlock = {
        // Send response that an error occurred, original exception is re-thrown
        out.writeInt(-1)
      }, finallyBlock = {
        out.close()
        in.close()
      })
    }

    val server = new SocketFuncServer(authHelper, "serve toLocalIterator", handleFunc)
    Array(server.port, server.secret, server)
  }

  def readRDDFromFile(
    sc: JavaSparkContext,
    filename: String,
    parallelism: Int): JavaRDD[Array[Byte]] = {
    JavaRDD.readRDDFromFile(sc, filename, parallelism)
  }

  def readRDDFromInputStream(
    sc: SparkContext,
    in: InputStream,
    parallelism: Int): JavaRDD[Array[Byte]] = {
    JavaRDD.readRDDFromInputStream(sc, in, parallelism)
  }

  def setupBroadcast(path: String): PythonBroadcast = {
    new PythonBroadcast(path)
  }

  def writeIteratorToStream[T](iter: Iterator[T], dataOut: DataOutputStream): Unit = {

    def write(obj: Any): Unit = obj match {
      case null =>
        dataOut.writeInt(SpecialLengths.NULL)
      case arr: Array[Byte] =>
        dataOut.writeInt(arr.length)
        dataOut.write(arr)
      case str: String =>
        writeUTF(str, dataOut)
      case stream: PortableDataStream =>
        write(stream.toArray())
      case (key, value) =>
        write(key)
        write(value)
      case other =>
        throw new SparkException("Unexpected element type " + other.getClass)
    }

    iter.foreach(write)
  }

  /**
   * Create an RDD from a path using [[org.apache.hadoop.mapred.SequenceFileInputFormat]],
   * key and value class.
   * A key and/or value converter class can optionally be passed in
   * (see [[org.apache.spark.api.python.Converter]])
   */
  def sequenceFile[K, V](
    sc: JavaSparkContext,
    path: String,
    keyClassMaybeNull: String,
    valueClassMaybeNull: String,
    keyConverterClass: String,
    valueConverterClass: String,
    minSplits: Int,
    batchSize: Int): JavaRDD[Array[Byte]] = {
    val keyClass = Option(keyClassMaybeNull).getOrElse("org.apache.hadoop.io.Text")
    val valueClass = Option(valueClassMaybeNull).getOrElse("org.apache.hadoop.io.Text")
    val kc = Utils.classForName[K](keyClass)
    val vc = Utils.classForName[V](valueClass)
    val rdd = sc.sc.sequenceFile[K, V](path, kc, vc, minSplits)
    val confBroadcasted = sc.sc.broadcast(new SerializableConfiguration(sc.hadoopConfiguration()))
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new WritableToJavaConverter(confBroadcasted))
    JavaRDD.fromRDD(SerDeUtil.pairRDDToPython(converted, batchSize))
  }

  /**
   * Create an RDD from a file path, using an arbitrary [[org.apache.hadoop.mapreduce.InputFormat]],
   * key and value class.
   * A key and/or value converter class can optionally be passed in
   * (see [[org.apache.spark.api.python.Converter]])
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
    sc: JavaSparkContext,
    path: String,
    inputFormatClass: String,
    keyClass: String,
    valueClass: String,
    keyConverterClass: String,
    valueConverterClass: String,
    confAsMap: java.util.HashMap[String, String],
    batchSize: Int): JavaRDD[Array[Byte]] = {
    val mergedConf = getMergedConf(confAsMap, sc.hadoopConfiguration())
    val rdd =
      newAPIHadoopRDDFromClassNames[K, V, F](sc,
        Some(path), inputFormatClass, keyClass, valueClass, mergedConf)
    val confBroadcasted = sc.sc.broadcast(new SerializableConfiguration(mergedConf))
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new WritableToJavaConverter(confBroadcasted))
    JavaRDD.fromRDD(SerDeUtil.pairRDDToPython(converted, batchSize))
  }

  /**
   * Create an RDD from a [[org.apache.hadoop.conf.Configuration]] converted from a map that is
   * passed in from Python, using an arbitrary [[org.apache.hadoop.mapreduce.InputFormat]],
   * key and value class.
   * A key and/or value converter class can optionally be passed in
   * (see [[org.apache.spark.api.python.Converter]])
   */
  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
    sc: JavaSparkContext,
    inputFormatClass: String,
    keyClass: String,
    valueClass: String,
    keyConverterClass: String,
    valueConverterClass: String,
    confAsMap: java.util.HashMap[String, String],
    batchSize: Int): JavaRDD[Array[Byte]] = {
    val conf = getMergedConf(confAsMap, sc.hadoopConfiguration())
    val rdd =
      newAPIHadoopRDDFromClassNames[K, V, F](sc,
        None, inputFormatClass, keyClass, valueClass, conf)
    val confBroadcasted = sc.sc.broadcast(new SerializableConfiguration(conf))
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new WritableToJavaConverter(confBroadcasted))
    JavaRDD.fromRDD(SerDeUtil.pairRDDToPython(converted, batchSize))
  }

  private def newAPIHadoopRDDFromClassNames[K, V, F <: NewInputFormat[K, V]](
    sc: JavaSparkContext,
    path: Option[String] = None,
    inputFormatClass: String,
    keyClass: String,
    valueClass: String,
    conf: Configuration): RDD[(K, V)] = {
    val kc = Utils.classForName[K](keyClass)
    val vc = Utils.classForName[V](valueClass)
    val fc = Utils.classForName[F](inputFormatClass)
    if (path.isDefined) {
      sc.sc.newAPIHadoopFile[K, V, F](path.get, fc, kc, vc, conf)
    } else {
      sc.sc.newAPIHadoopRDD[K, V, F](conf, fc, kc, vc)
    }
  }

  /**
   * Create an RDD from a file path, using an arbitrary [[org.apache.hadoop.mapred.InputFormat]],
   * key and value class.
   * A key and/or value converter class can optionally be passed in
   * (see [[org.apache.spark.api.python.Converter]])
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](
    sc: JavaSparkContext,
    path: String,
    inputFormatClass: String,
    keyClass: String,
    valueClass: String,
    keyConverterClass: String,
    valueConverterClass: String,
    confAsMap: java.util.HashMap[String, String],
    batchSize: Int): JavaRDD[Array[Byte]] = {
    val mergedConf = getMergedConf(confAsMap, sc.hadoopConfiguration())
    val rdd =
      hadoopRDDFromClassNames[K, V, F](sc,
        Some(path), inputFormatClass, keyClass, valueClass, mergedConf)
    val confBroadcasted = sc.sc.broadcast(new SerializableConfiguration(mergedConf))
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new WritableToJavaConverter(confBroadcasted))
    JavaRDD.fromRDD(SerDeUtil.pairRDDToPython(converted, batchSize))
  }

  /**
   * Create an RDD from a [[org.apache.hadoop.conf.Configuration]] converted from a map
   * that is passed in from Python, using an arbitrary [[org.apache.hadoop.mapred.InputFormat]],
   * key and value class
   * A key and/or value converter class can optionally be passed in
   * (see [[org.apache.spark.api.python.Converter]])
   */
  def hadoopRDD[K, V, F <: InputFormat[K, V]](
    sc: JavaSparkContext,
    inputFormatClass: String,
    keyClass: String,
    valueClass: String,
    keyConverterClass: String,
    valueConverterClass: String,
    confAsMap: java.util.HashMap[String, String],
    batchSize: Int): JavaRDD[Array[Byte]] = {
    val conf = getMergedConf(confAsMap, sc.hadoopConfiguration())
    val rdd =
      hadoopRDDFromClassNames[K, V, F](sc,
        None, inputFormatClass, keyClass, valueClass, conf)
    val confBroadcasted = sc.sc.broadcast(new SerializableConfiguration(conf))
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new WritableToJavaConverter(confBroadcasted))
    JavaRDD.fromRDD(SerDeUtil.pairRDDToPython(converted, batchSize))
  }

  private def hadoopRDDFromClassNames[K, V, F <: InputFormat[K, V]](
    sc: JavaSparkContext,
    path: Option[String] = None,
    inputFormatClass: String,
    keyClass: String,
    valueClass: String,
    conf: Configuration) = {
    val kc = Utils.classForName[K](keyClass)
    val vc = Utils.classForName[V](valueClass)
    val fc = Utils.classForName[F](inputFormatClass)
    if (path.isDefined) {
      sc.sc.hadoopFile(path.get, fc, kc, vc)
    } else {
      sc.sc.hadoopRDD(new JobConf(conf), fc, kc, vc)
    }
  }

  def writeUTF(str: String, dataOut: DataOutputStream): Unit = {
    val bytes = str.getBytes(StandardCharsets.UTF_8)
    dataOut.writeInt(bytes.length)
    dataOut.write(bytes)
  }

  /**
   * Create a socket server and a background thread to serve the data in `items`,
   *
   * The socket server can only accept one connection, or close if no connection
   * in 15 seconds.
   *
   * Once a connection comes in, it tries to serialize all the data in `items`
   * and send them into this connection.
   *
   * The thread will terminate after all the data are sent or any exceptions happen.
   *
   * @return 3-tuple (as a Java array) with the port number of a local socket which serves the
   *         data collected from this job, the secret for authentication, and a socket auth
   *         server object that can be used to join the JVM serving thread in Python.
   */
  def serveIterator(items: Iterator[_], threadName: String): Array[Any] = {
    serveToStream(threadName) { out =>
      writeIteratorToStream(items, new DataOutputStream(out))
    }
  }

  /**
   * Create a socket server and background thread to execute the writeFunc
   * with the given OutputStream.
   *
   * The socket server can only accept one connection, or close if no connection
   * in 15 seconds.
   *
   * Once a connection comes in, it will execute the block of code and pass in
   * the socket output stream.
   *
   * The thread will terminate after the block of code is executed or any
   * exceptions happen.
   *
   * @return 3-tuple (as a Java array) with the port number of a local socket which serves the
   *         data collected from this job, the secret for authentication, and a socket auth
   *         server object that can be used to join the JVM serving thread in Python.
   */
  private[spark] def serveToStream(
    threadName: String)(writeFunc: OutputStream => Unit): Array[Any] = {
    SocketAuthServer.serveToStream(threadName, authHelper)(writeFunc)
  }

  private def getMergedConf(confAsMap: java.util.HashMap[String, String],
    baseConf: Configuration): Configuration = {
    val conf = PythonHadoopUtil.mapToConf(confAsMap)
    PythonHadoopUtil.mergeConfs(baseConf, conf)
  }

  private def inferKeyValueTypes[K, V, KK, VV](rdd: RDD[(K, V)], keyConverterClass: String = null,
    valueConverterClass: String = null): (Class[_ <: KK], Class[_ <: VV]) = {
    // Peek at an element to figure out key/value types. Since Writables are not serializable,
    // we cannot call first() on the converted RDD. Instead, we call first() on the original RDD
    // and then convert locally.
    val (key, value) = rdd.first()
    val (kc, vc) = getKeyValueConverters[K, V, KK, VV](
      keyConverterClass, valueConverterClass, new JavaToWritableConverter)
    (kc.convert(key).getClass, vc.convert(value).getClass)
  }

  private def getKeyValueTypes[K, V](keyClass: String, valueClass: String):
  Option[(Class[K], Class[V])] = {
    for {
      k <- Option(keyClass)
      v <- Option(valueClass)
    } yield (Utils.classForName(k), Utils.classForName(v))
  }

  private def getKeyValueConverters[K, V, KK, VV](
    keyConverterClass: String,
    valueConverterClass: String,
    defaultConverter: Converter[_, _]): (Converter[K, KK], Converter[V, VV]) = {
    val keyConverter = Converter.getInstance(Option(keyConverterClass),
      defaultConverter.asInstanceOf[Converter[K, KK]])
    val valueConverter = Converter.getInstance(Option(valueConverterClass),
      defaultConverter.asInstanceOf[Converter[V, VV]])
    (keyConverter, valueConverter)
  }

  /**
   * Convert an RDD of key-value pairs from internal types to serializable types suitable for
   * output, or vice versa.
   */
  private def convertRDD[K, V](rdd: RDD[(K, V)],
    keyConverterClass: String,
    valueConverterClass: String,
    defaultConverter: Converter[Any, Any]): RDD[(Any, Any)] = {
    val (kc, vc) = getKeyValueConverters[K, V, Any, Any](keyConverterClass, valueConverterClass,
      defaultConverter)
    PythonHadoopUtil.convertRDD(rdd, kc, vc)
  }

  /**
   * Output a Python RDD of key-value pairs as a Hadoop SequenceFile using the Writable types
   * we convert from the RDD's key and value types. Note that keys and values can't be
   * [[org.apache.hadoop.io.Writable]] types already, since Writables are not Java
   * `Serializable` and we can't peek at them. The `path` can be on any Hadoop file system.
   */
  def saveAsSequenceFile[C <: CompressionCodec](
    pyRDD: JavaRDD[Array[Byte]],
    batchSerialized: Boolean,
    path: String,
    compressionCodecClass: String): Unit = {
    saveAsHadoopFile(
      pyRDD, batchSerialized, path, "org.apache.hadoop.mapred.SequenceFileOutputFormat",
      null, null, null, null, new java.util.HashMap(), compressionCodecClass)
  }

  /**
   * Output a Python RDD of key-value pairs to any Hadoop file system, using old Hadoop
   * `OutputFormat` in mapred package. Keys and values are converted to suitable output
   * types using either user specified converters or, if not specified,
   * [[org.apache.spark.api.python.JavaToWritableConverter]]. Post-conversion types
   * `keyClass` and `valueClass` are automatically inferred if not specified. The passed-in
   * `confAsMap` is merged with the default Hadoop conf associated with the SparkContext of
   * this RDD.
   */
  def saveAsHadoopFile[F <: OutputFormat[_, _], C <: CompressionCodec](
    pyRDD: JavaRDD[Array[Byte]],
    batchSerialized: Boolean,
    path: String,
    outputFormatClass: String,
    keyClass: String,
    valueClass: String,
    keyConverterClass: String,
    valueConverterClass: String,
    confAsMap: java.util.HashMap[String, String],
    compressionCodecClass: String): Unit = {
    val rdd = SerDeUtil.pythonToPairRDD(pyRDD, batchSerialized)
    val (kc, vc) = getKeyValueTypes(keyClass, valueClass).getOrElse(
      inferKeyValueTypes(rdd, keyConverterClass, valueConverterClass))
    val mergedConf = getMergedConf(confAsMap, pyRDD.context.hadoopConfiguration)
    val codec = Option(compressionCodecClass).map(Utils.classForName(_).asInstanceOf[Class[C]])
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new JavaToWritableConverter)
    val fc = Utils.classForName[F](outputFormatClass)
    converted.saveAsHadoopFile(path, kc, vc, fc, new JobConf(mergedConf), codec = codec)
  }

  /**
   * Output a Python RDD of key-value pairs to any Hadoop file system, using new Hadoop
   * `OutputFormat` in mapreduce package. Keys and values are converted to suitable output
   * types using either user specified converters or, if not specified,
   * [[org.apache.spark.api.python.JavaToWritableConverter]]. Post-conversion types
   * `keyClass` and `valueClass` are automatically inferred if not specified. The passed-in
   * `confAsMap` is merged with the default Hadoop conf associated with the SparkContext of
   * this RDD.
   */
  def saveAsNewAPIHadoopFile[F <: NewOutputFormat[_, _]](
    pyRDD: JavaRDD[Array[Byte]],
    batchSerialized: Boolean,
    path: String,
    outputFormatClass: String,
    keyClass: String,
    valueClass: String,
    keyConverterClass: String,
    valueConverterClass: String,
    confAsMap: java.util.HashMap[String, String]): Unit = {
    val rdd = SerDeUtil.pythonToPairRDD(pyRDD, batchSerialized)
    val (kc, vc) = getKeyValueTypes(keyClass, valueClass).getOrElse(
      inferKeyValueTypes(rdd, keyConverterClass, valueConverterClass))
    val mergedConf = getMergedConf(confAsMap, pyRDD.context.hadoopConfiguration)
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new JavaToWritableConverter)
    val fc = Utils.classForName(outputFormatClass).asInstanceOf[Class[F]]
    converted.saveAsNewAPIHadoopFile(path, kc, vc, fc, mergedConf)
  }

  /**
   * Output a Python RDD of key-value pairs to any Hadoop file system, using a Hadoop conf
   * converted from the passed-in `confAsMap`. The conf should set relevant output params (
   * e.g., output path, output format, etc), in the same way as it would be configured for
   * a Hadoop MapReduce job. Both old and new Hadoop OutputFormat APIs are supported
   * (mapred vs. mapreduce). Keys/values are converted for output using either user specified
   * converters or, by default, [[org.apache.spark.api.python.JavaToWritableConverter]].
   */
  def saveAsHadoopDataset(
    pyRDD: JavaRDD[Array[Byte]],
    batchSerialized: Boolean,
    confAsMap: java.util.HashMap[String, String],
    keyConverterClass: String,
    valueConverterClass: String,
    useNewAPI: Boolean): Unit = {
    val conf = getMergedConf(confAsMap, pyRDD.context.hadoopConfiguration)
    val converted = convertRDD(SerDeUtil.pythonToPairRDD(pyRDD, batchSerialized),
      keyConverterClass, valueConverterClass, new JavaToWritableConverter)
    if (useNewAPI) {
      converted.saveAsNewAPIHadoopDataset(conf)
    } else {
      converted.saveAsHadoopDataset(new JobConf(conf))
    }
  }
}

