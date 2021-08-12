//
//package org.apache.spark.executor
//
//import org.apache.spark.deploy.SparkHadoopUtil
//import org.apache.spark.deploy.worker.WorkerWatcher
//import org.apache.spark.executor.CoarseGrainedExecutorBackend.RegisteredExecutor
//import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}
//import org.apache.spark.internal.Logging
//import org.apache.spark.internal.config.EXECUTOR_ID
//import org.apache.spark.resource.ResourceProfile
//import org.apache.spark.resource.ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
//import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
//import org.apache.spark.scheduler.TaskDescription
//import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{KillTask, LaunchTask, LaunchedExecutor, RetrieveSparkAppConfig, Shutdown, SparkAppConfig, StopExecutor, UpdateDelegationTokens}
//import org.apache.spark.util.Utils
//
//import java.net.URL
//import scala.collection.mutable
//import scala.util.control.NonFatal
//
//private[spark] class BlazeExecutorBackend(
//                                           override val rpcEnv: RpcEnv,
//                                           driverUrl: String,
//                                           executorId: String,
//                                           bindAddress: String,
//                                           hostname: String,
//                                           cores: Int,
//                                           userClassPath: Seq[URL],
//                                           env: SparkEnv,
//                                           resourcesFileOpt: Option[String],
//                                           resourceProfile: ResourceProfile) extends CoarseGrainedExecutorBackend(rpcEnv,
//  driverUrl, executorId, bindAddress, hostname, cores,
//  userClassPath, env, resourcesFileOpt, resourceProfile) {
//
//  override def receive: PartialFunction[Any, Unit] = {
//    case RegisteredExecutor =>
//      logInfo("Successfully registered with driver")
//      try {
//        executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false,
//          resources = _resources)
//        driver.get.send(LaunchedExecutor(executorId))
//      } catch {
//        case NonFatal(e) =>
//          exitExecutor(1, "Unable to create executor due to " + e.getMessage, e)
//      }
//
//    case LaunchTask(data) =>
//      if (executor == null) {
//        exitExecutor(1, "Received LaunchTask command but executor was null")
//      } else {
//        val taskDesc = TaskDescription.decode(data.value)
//        logInfo("Got assigned task " + taskDesc.taskId)
//        taskResources(taskDesc.taskId) = taskDesc.resources
//        executor.launchTask(this, taskDesc)
//      }
//
//    case KillTask(taskId, _, interruptThread, reason) =>
//      if (executor == null) {
//        exitExecutor(1, "Received KillTask command but executor was null")
//      } else {
//        executor.killTask(taskId, interruptThread, reason)
//      }
//
//    case StopExecutor =>
//      stopping.set(true)
//      logInfo("Driver commanded a shutdown")
//      // Cannot shutdown here because an ack may need to be sent back to the caller. So send
//      // a message to self to actually do the shutdown.
//      self.send(Shutdown)
//
//    case Shutdown =>
//      stopping.set(true)
//      new Thread("CoarseGrainedExecutorBackend-stop-executor") {
//        override def run(): Unit = {
//          // executor.stop() will call `SparkEnv.stop()` which waits until RpcEnv stops totally.
//          // However, if `executor.stop()` runs in some thread of RpcEnv, RpcEnv won't be able to
//          // stop until `executor.stop()` returns, which becomes a dead-lock (See SPARK-14180).
//          // Therefore, we put this line in a new thread.
//          executor.stop()
//        }
//      }.start()
//
//    case UpdateDelegationTokens(tokenBytes) =>
//      logInfo(s"Received tokens of ${tokenBytes.length} bytes")
//      SparkHadoopUtil.get.addDelegationTokens(tokenBytes, env.conf)
//  }
//
//
//}
//
//private[spark] object BlazeExecutorBackend extends Logging {
//
//  // Message used internally to start the executor when the driver successfully accepted the
//  // registration request.
//  case object RegisteredExecutor
//
//  case class Arguments(
//                        driverUrl: String,
//                        executorId: String,
//                        bindAddress: String,
//                        hostname: String,
//                        cores: Int,
//                        appId: String,
//                        workerUrl: Option[String],
//                        userClassPath: mutable.ListBuffer[URL],
//                        resourcesFileOpt: Option[String],
//                        resourceProfileId: Int)
//
//  def main(args: Array[String]): Unit = {
//    val createFn: (RpcEnv, Arguments, SparkEnv, ResourceProfile) =>
//      BlazeExecutorBackend = {
//      case (rpcEnv, arguments, env, resourceProfile) =>
//        new BlazeExecutorBackend(rpcEnv, arguments.driverUrl, arguments.executorId,
//          arguments.bindAddress, arguments.hostname, arguments.cores, arguments.userClassPath, env,
//          arguments.resourcesFileOpt, resourceProfile)
//    }
//    run(parseArguments(args, this.getClass.getCanonicalName.stripSuffix("$")), createFn)
//    System.exit(0)
//  }
//
//  def run(
//           arguments: Arguments,
//           backendCreateFn: (RpcEnv, Arguments, SparkEnv, ResourceProfile) =>
//             BlazeExecutorBackend): Unit = {
//
//    Utils.initDaemon(log)
//
//    SparkHadoopUtil.get.runAsSparkUser { () =>
//      // Debug code
//      Utils.checkHost(arguments.hostname)
//
//      // Bootstrap to fetch the driver's Spark properties.
//      val executorConf = new SparkConf
//      val fetcher = RpcEnv.create(
//        "driverPropsFetcher",
//        arguments.bindAddress,
//        arguments.hostname,
//        -1,
//        executorConf,
//        new SecurityManager(executorConf),
//        numUsableCores = 0,
//        clientMode = true)
//
//      var driver: RpcEndpointRef = null
//      val nTries = 3
//      for (i <- 0 until nTries if driver == null) {
//        try {
//          driver = fetcher.setupEndpointRefByURI(arguments.driverUrl)
//        } catch {
//          case e: Throwable => if (i == nTries - 1) {
//            throw e
//          }
//        }
//      }
//
//      val cfg = driver.askSync[SparkAppConfig](RetrieveSparkAppConfig(arguments.resourceProfileId))
//      val props = cfg.sparkProperties ++ Seq[(String, String)](("spark.app.id", arguments.appId))
//      fetcher.shutdown()
//
//      // Create SparkEnv using properties we fetched from the driver.
//      val driverConf = new SparkConf()
//      for ((key, value) <- props) {
//        // this is required for SSL in standalone mode
//        if (SparkConf.isExecutorStartupConf(key)) {
//          driverConf.setIfMissing(key, value)
//        } else {
//          driverConf.set(key, value)
//        }
//      }
//
//      cfg.hadoopDelegationCreds.foreach { tokens =>
//        SparkHadoopUtil.get.addDelegationTokens(tokens, driverConf)
//      }
//
//      driverConf.set(EXECUTOR_ID, arguments.executorId)
//      val env = SparkEnv.createExecutorEnv(driverConf, arguments.executorId, arguments.bindAddress,
//        arguments.hostname, arguments.cores, cfg.ioEncryptionKey, isLocal = false)
//
//      env.rpcEnv.setupEndpoint("Executor",
//        backendCreateFn(env.rpcEnv, arguments, env, cfg.resourceProfile))
//      arguments.workerUrl.foreach { url =>
//        env.rpcEnv.setupEndpoint("WorkerWatcher", new WorkerWatcher(env.rpcEnv, url))
//      }
//      env.rpcEnv.awaitTermination()
//    }
//  }
//
//  def parseArguments(args: Array[String], classNameForEntry: String): Arguments = {
//    var driverUrl: String = null
//    var executorId: String = null
//    var bindAddress: String = null
//    var hostname: String = null
//    var cores: Int = 0
//    var resourcesFileOpt: Option[String] = None
//    var appId: String = null
//    var workerUrl: Option[String] = None
//    val userClassPath = new mutable.ListBuffer[URL]()
//    var resourceProfileId: Int = DEFAULT_RESOURCE_PROFILE_ID
//
//    var argv = args.toList
//    while (!argv.isEmpty) {
//      argv match {
//        case ("--driver-url") :: value :: tail =>
//          driverUrl = value
//          argv = tail
//        case ("--executor-id") :: value :: tail =>
//          executorId = value
//          argv = tail
//        case ("--bind-address") :: value :: tail =>
//          bindAddress = value
//          argv = tail
//        case ("--hostname") :: value :: tail =>
//          hostname = value
//          argv = tail
//        case ("--cores") :: value :: tail =>
//          cores = value.toInt
//          argv = tail
//        case ("--resourcesFile") :: value :: tail =>
//          resourcesFileOpt = Some(value)
//          argv = tail
//        case ("--app-id") :: value :: tail =>
//          appId = value
//          argv = tail
//        case ("--worker-url") :: value :: tail =>
//          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
//          workerUrl = Some(value)
//          argv = tail
//        case ("--user-class-path") :: value :: tail =>
//          userClassPath += new URL(value)
//          argv = tail
//        case ("--resourceProfileId") :: value :: tail =>
//          resourceProfileId = value.toInt
//          argv = tail
//        case Nil =>
//        case tail =>
//          // scalastyle:off println
//          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
//          // scalastyle:on println
//          printUsageAndExit(classNameForEntry)
//      }
//    }
//
//    if (hostname == null) {
//      hostname = Utils.localHostName()
//      log.info(s"Executor hostname is not provided, will use '$hostname' to advertise itself")
//    }
//
//    if (driverUrl == null || executorId == null || cores <= 0 || appId == null) {
//      printUsageAndExit(classNameForEntry)
//    }
//
//    if (bindAddress == null) {
//      bindAddress = hostname
//    }
//
//    Arguments(driverUrl, executorId, bindAddress, hostname, cores, appId, workerUrl,
//      userClassPath, resourcesFileOpt, resourceProfileId)
//  }
//
//  private def printUsageAndExit(classNameForEntry: String): Unit = {
//    // scalastyle:off println
//    System.err.println(
//      s"""
//         |Usage: $classNameForEntry [options]
//         |
//         | Options are:
//         |   --driver-url <driverUrl>
//         |   --executor-id <executorId>
//         |   --bind-address <bindAddress>
//         |   --hostname <hostname>
//         |   --cores <cores>
//         |   --resourcesFile <fileWithJSONResourceInformation>
//         |   --app-id <appid>
//         |   --worker-url <workerUrl>
//         |   --user-class-path <url>
//         |   --resourceProfileId <id>
//         |""".stripMargin)
//    // scalastyle:on println
//    System.exit(1)
//  }
//}
