//
//package org.apache.spark.executor
//
//import com.google.common.util.concurrent.ThreadFactoryBuilder
//import org.apache.spark.{HeartbeatReceiver, Heartbeater, SparkConf, SparkEnv, SparkException}
//import org.apache.spark.internal.config.{EXECUTOR_HEARTBEAT_DROP_ZERO_ACCUMULATOR_UPDATES, EXECUTOR_HEARTBEAT_INTERVAL, EXECUTOR_HEARTBEAT_MAX_FAILURES, EXECUTOR_METRICS_POLLING_INTERVAL, EXECUTOR_USER_CLASS_PATH_FIRST, MAX_RESULT_SIZE, METRICS_EXECUTORMETRICS_SOURCE_ENABLED, TASK_MAX_DIRECT_RESULT_SIZE, TASK_REAPER_ENABLED, TASK_REAPER_KILL_TIMEOUT, TASK_REAPER_POLLING_INTERVAL, TASK_REAPER_THREAD_DUMP}
//import org.apache.spark.internal.plugin.PluginContainer
//import org.apache.spark.resource.ResourceInformation
//import org.apache.spark.util.{AccumulatorV2, ChildFirstURLClassLoader, MutableURLClassLoader, RpcUtils, ShutdownHookManager, SparkUncaughtExceptionHandler, ThreadUtils, UninterruptibleThread, Utils}
//
//import java.io.File
//import java.lang.Thread.UncaughtExceptionHandler
//import java.lang.management.ManagementFactory
//import java.net.URL
//import java.nio.ByteBuffer
//import java.util.concurrent.{ConcurrentHashMap, Executors, ThreadPoolExecutor, TimeUnit}
//import java.util.concurrent.atomic.AtomicBoolean
//import scala.collection.JavaConverters._
//import scala.collection.{immutable, mutable}
//import scala.collection.mutable.{ArrayBuffer, HashMap, Map}
//import scala.util.control.NonFatal
//
//class BlazeExecutor(
//         executorId: String,
//         executorHostname: String,
//         env: SparkEnv,
//         userClassPath: Seq[URL] = Nil,
//         isLocal: Boolean = false,
//         uncaughtExceptionHandler: UncaughtExceptionHandler = new SparkUncaughtExceptionHandler,
//         resources: immutable.Map[String, ResourceInformation])
//  extends Executor(executorId, executorHostname, env, userClassPath,
//    isLocal, uncaughtExceptionHandler, resources) {
//
//
//  logInfo(s"Starting executor ID $executorId on host $executorHostname")
//
//  private val executorShutdown = new AtomicBoolean(false)
//  ShutdownHookManager.addShutdownHook(
//    () => stop()
//  )
//  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
//  // Each map holds the master's timestamp for the version of that file or JAR we got.
//  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
//  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()
//
//  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))
//
//  private val conf = env.conf
//
//  // No ip or host:port - just hostname
//  Utils.checkHost(executorHostname)
//  // must not have port specified.
//  assert(0 == Utils.parseHostPort(executorHostname)._2)
//
//  // Make sure the local hostname we report matches the cluster scheduler's name for this host
//  Utils.setCustomHostname(executorHostname)
//
//  if (!isLocal) {
//    // Setup an uncaught exception handler for non-local mode.
//    // Make any thread terminations due to uncaught exceptions kill the entire
//    // executor process to avoid surprising stalls.
//    Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler)
//  }
//
//  // Start worker thread pool
//  // Use UninterruptibleThread to run tasks so that we can allow running codes without being
//  // interrupted by `Thread.interrupt()`. Some issues, such as KAFKA-1894, HADOOP-10622,
//  // will hang forever if some methods are interrupted.
//  private val threadPool = {
//    val threadFactory = new ThreadFactoryBuilder()
//      .setDaemon(true)
//      .setNameFormat("Executor task launch worker-%d")
//      .setThreadFactory((r: Runnable) => new UninterruptibleThread(r, "unused"))
//      .build()
//    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
//  }
//  private val executorSource = new ExecutorSource(threadPool, executorId)
//  // Pool used for threads that supervise task killing / cancellation
//  private val taskReaperPool = ThreadUtils.newDaemonCachedThreadPool("Task reaper")
//  // For tasks which are in the process of being killed, this map holds the most recently created
//  // TaskReaper. All accesses to this map should be synchronized on the map itself (this isn't
//  // a ConcurrentHashMap because we use the synchronization for purposes other than simply guarding
//  // the integrity of the map's internal state). The purpose of this map is to prevent the creation
//  // of a separate TaskReaper for every killTask() of a given task. Instead, this map allows us to
//  // track whether an existing TaskReaper fulfills the role of a TaskReaper that we would otherwise
//  // create. The map key is a task id.
//  private val taskReaperForTask: HashMap[Long, TaskReaper] = HashMap[Long, TaskReaper]()
//
//  // Whether to load classes in user jars before those in Spark jars
//  private val userClassPathFirst = conf.get(EXECUTOR_USER_CLASS_PATH_FIRST)
//
//  // Whether to monitor killed / interrupted tasks
//  private val taskReaperEnabled = conf.get(TASK_REAPER_ENABLED)
//
//  // Create our ClassLoader
//  // do this after SparkEnv creation so can access the SecurityManager
//  private val urlClassLoader = createClassLoader()
//  private val replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)
//
//  // Set the classloader for serializer
//  env.serializer.setDefaultClassLoader(replClassLoader)
//  // SPARK-21928.  SerializerManager's internal instance of Kryo might get used in netty threads
//  // for fetching remote cached RDD blocks, so need to make sure it uses the right classloader too.
//  env.serializerManager.setDefaultClassLoader(replClassLoader)
//
//  // Max size of direct result. If task result is bigger than this, we use the block manager
//  // to send the result back.
//  private val maxDirectResultSize = Math.min(
//    conf.get(TASK_MAX_DIRECT_RESULT_SIZE),
//    RpcUtils.maxMessageSizeBytes(conf))
//
//  private val maxResultSize = conf.get(MAX_RESULT_SIZE)
//
//  // Maintains the list of running tasks.
//  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]
//
//  /**
//   * When an executor is unable to send heartbeats to the driver more than `HEARTBEAT_MAX_FAILURES`
//   * times, it should kill itself. The default value is 60. For example, if max failures is 60 and
//   * heartbeat interval is 10s, then it will try to send heartbeats for up to 600s (10 minutes).
//   */
//  private val HEARTBEAT_MAX_FAILURES = conf.get(EXECUTOR_HEARTBEAT_MAX_FAILURES)
//
//  /**
//   * Whether to drop empty accumulators from heartbeats sent to the driver. Including the empty
//   * accumulators (that satisfy isZero) can make the size of the heartbeat message very large.
//   */
//  private val HEARTBEAT_DROP_ZEROES = conf.get(EXECUTOR_HEARTBEAT_DROP_ZERO_ACCUMULATOR_UPDATES)
//
//  /**
//   * Interval to send heartbeats, in milliseconds
//   */
//  private val HEARTBEAT_INTERVAL_MS = conf.get(EXECUTOR_HEARTBEAT_INTERVAL)
//
//  /**
//   * Interval to poll for executor metrics, in milliseconds
//   */
//  private val METRICS_POLLING_INTERVAL_MS = conf.get(EXECUTOR_METRICS_POLLING_INTERVAL)
//
//  private val pollOnHeartbeat = if (METRICS_POLLING_INTERVAL_MS > 0) false else true
//
//  private val heartbeater = new Heartbeater(
//    () => BlazeExecutor.this.reportHeartBeat(),
//    "executor-heartbeater",
//    HEARTBEAT_INTERVAL_MS)
//
//  // must be initialized before running startDriverHeartbeat()
//  private val heartbeatReceiverRef =
//    RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)
//
//  /**
//   * Count the failure times of heartbeat. It should only be accessed in the heartbeat thread. Each
//   * successful heartbeat will reset it to 0.
//   */
//  private var heartbeatFailures = 0
//
//  heartbeater.start()
//
//  private val appStartTime = conf.getLong("spark.app.startTime", 0)
//
//  // To allow users to distribute plugins and their required files
//  // specified by --jars and --files on application submission, those jars/files should be
//  // downloaded and added to the class loader via updateDependencies.
//  // This should be done before plugin initialization below
//  // because executors search plugins from the class loader and initialize them.
//  private val Seq(initialUserJars, initialUserFiles) = Seq("jar", "file").map { key =>
//    conf.getOption(s"spark.app.initial.$key.urls").map { urls =>
//      Map(urls.split(",").map(url => (url, appStartTime)): _*)
//    }.getOrElse(Map.empty)
//  }
//
//  // Plugins need to load using a class loader that includes the executor's user classpath.
//  // Plugins also needs to be initialized after the heartbeater started
//  // to avoid blocking to send heartbeat (see SPARK-32175).
//  private val plugins: Option[PluginContainer] = Utils.withContextClassLoader(replClassLoader) {
//    PluginContainer(env, resources.asJava)
//  }
//
//  metricsPoller.start()
//
//
//  override def stop(): Unit = {
//    if (!executorShutdown.getAndSet(true)) {
//      env.metricsSystem.report()
//      try {
//        metricsPoller.stop()
//      } catch {
//        case NonFatal(e) =>
//          logWarning("Unable to stop executor metrics poller", e)
//      }
//      try {
//        heartbeater.stop()
//      } catch {
//        case NonFatal(e) =>
//          logWarning("Unable to stop heartbeater", e)
//      }
//      threadPool.shutdown()
//
//      // Notify plugins that executor is shutting down so they can terminate cleanly
//      Utils.withContextClassLoader(replClassLoader) {
//        plugins.foreach(_.shutdown())
//      }
//      if (!isLocal) {
//        env.stop()
//      }
//    }
//
//  }
//
//  private def computeTotalGcTime(): Long = {
//    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
//  }
//
//  private def reportHeartBeat(): Unit = {
//    // list of (task id, accumUpdates) to send back to the driver
//    val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]()
//    val curGCTime = computeTotalGcTime()
//
//    if (pollOnHeartbeat) {
//      metricsPoller.poll()
//    }
//
//    val executorUpdates = metricsPoller.getExecutorUpdates()
//
//    for (taskRunner <- runningTasks.values().asScala) {
//      if (taskRunner.task != null) {
//        taskRunner.task.metrics.mergeShuffleReadMetrics()
//        taskRunner.task.metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)
//        val accumulatorsToReport =
//          if (HEARTBEAT_DROP_ZEROES) {
//            taskRunner.task.metrics.accumulators().filterNot(_.isZero)
//          } else {
//            taskRunner.task.metrics.accumulators()
//          }
//        accumUpdates += ((taskRunner.taskId, accumulatorsToReport))
//      }
//    }
//  }
//
//
//  private class TaskReaper(
//                            taskRunner: TaskRunner,
//                            val interruptThread: Boolean,
//                            val reason: String)
//    extends Runnable {
//
//    private[this] val taskId: Long = taskRunner.taskId
//
//    private[this] val killPollingIntervalMs: Long = conf.get(TASK_REAPER_POLLING_INTERVAL)
//
//    private[this] val killTimeoutNs: Long = {
//      TimeUnit.MILLISECONDS.toNanos(conf.get(TASK_REAPER_KILL_TIMEOUT))
//    }
//
//    private[this] val takeThreadDump: Boolean = conf.get(TASK_REAPER_THREAD_DUMP)
//
//    override def run(): Unit = {
//      val startTimeNs = System.nanoTime()
//      def elapsedTimeNs = System.nanoTime() - startTimeNs
//      def timeoutExceeded(): Boolean = killTimeoutNs > 0 && elapsedTimeNs > killTimeoutNs
//      try {
//        // Only attempt to kill the task once. If interruptThread = false then a second kill
//        // attempt would be a no-op and if interruptThread = true then it may not be safe or
//        // effective to interrupt multiple times:
//        taskRunner.kill(interruptThread = interruptThread, reason = reason)
//        // Monitor the killed task until it exits. The synchronization logic here is complicated
//        // because we don't want to synchronize on the taskRunner while possibly taking a thread
//        // dump, but we also need to be careful to avoid races between checking whether the task
//        // has finished and wait()ing for it to finish.
//        var finished: Boolean = false
//        while (!finished && !timeoutExceeded()) {
//          taskRunner.synchronized {
//            // We need to synchronize on the TaskRunner while checking whether the task has
//            // finished in order to avoid a race where the task is marked as finished right after
//            // we check and before we call wait().
//            if (taskRunner.isFinished) {
//              finished = true
//            } else {
//              taskRunner.wait(killPollingIntervalMs)
//            }
//          }
//          if (taskRunner.isFinished) {
//            finished = true
//          } else {
//            val elapsedTimeMs = TimeUnit.NANOSECONDS.toMillis(elapsedTimeNs)
//            logWarning(s"Killed task $taskId is still running after $elapsedTimeMs ms")
//            if (takeThreadDump) {
//              try {
//                Utils.getThreadDumpForThread(taskRunner.getThreadId).foreach { thread =>
//                  if (thread.threadName == taskRunner.threadName) {
//                    logWarning(s"Thread dump from task $taskId:\n${thread.stackTrace}")
//                  }
//                }
//              } catch {
//                case NonFatal(e) =>
//                  logWarning("Exception thrown while obtaining thread dump: ", e)
//              }
//            }
//          }
//        }
//
//        if (!taskRunner.isFinished && timeoutExceeded()) {
//          val killTimeoutMs = TimeUnit.NANOSECONDS.toMillis(killTimeoutNs)
//          if (isLocal) {
//            logError(s"Killed task $taskId could not be stopped within $killTimeoutMs ms; " +
//              "not killing JVM because we are running in local mode.")
//          } else {
//            // In non-local-mode, the exception thrown here will bubble up to the uncaught exception
//            // handler and cause the executor JVM to exit.
//            throw new SparkException(
//              s"Killing executor JVM because killed task $taskId could not be stopped within " +
//                s"$killTimeoutMs ms.")
//          }
//        }
//      } finally {
//        // Clean up entries in the taskReaperForTask map.
//        taskReaperForTask.synchronized {
//          taskReaperForTask.get(taskId).foreach { taskReaperInMap =>
//            if (taskReaperInMap eq this) {
//              taskReaperForTask.remove(taskId)
//            } else {
//              // This must have been a TaskReaper where interruptThread == false where a subsequent
//              // killTask() call for the same task had interruptThread == true and overwrote the
//              // map entry.
//            }
//          }
//        }
//      }
//    }
//  }
//
//
//  /**
//   * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
//   * created by the interpreter to the search path
//   */
//  private def createClassLoader(): MutableURLClassLoader = {
//    // Bootstrap the list of jars with the user class path.
//    val now = System.currentTimeMillis()
//    userClassPath.foreach { url =>
//      currentJars(url.getPath().split("/").last) = now
//    }
//
//    val currentLoader = Utils.getContextOrSparkClassLoader
//
//    // For each of the jars in the jarSet, add them to the class loader.
//    // We assume each of the files has already been fetched.
//    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
//      new File(uri.split("/").last).toURI.toURL
//    }
//    if (userClassPathFirst) {
//      new ChildFirstURLClassLoader(urls, currentLoader)
//    } else {
//      new MutableURLClassLoader(urls, currentLoader)
//    }
//  }
//
//
//  /**
//   * If the REPL is in use, add another ClassLoader that will read
//   * new classes defined by the REPL as the user types code
//   */
//  private def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader = {
//    val classUri = conf.get("spark.repl.class.uri", null)
//    if (classUri != null) {
//      logInfo("Using REPL class URI: " + classUri)
//      try {
//        val _userClassPathFirst: java.lang.Boolean = userClassPathFirst
//        val klass = Utils.classForName("org.apache.spark.repl.ExecutorClassLoader")
//          .asInstanceOf[Class[_ <: ClassLoader]]
//        val constructor = klass.getConstructor(classOf[SparkConf], classOf[SparkEnv],
//          classOf[String], classOf[ClassLoader], classOf[Boolean])
//        constructor.newInstance(conf, env, classUri, parent, _userClassPathFirst)
//      } catch {
//        case _: ClassNotFoundException =>
//          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
//          System.exit(1)
//          null
//      }
//    } else {
//      parent
//    }
//  }
//
//
//}
