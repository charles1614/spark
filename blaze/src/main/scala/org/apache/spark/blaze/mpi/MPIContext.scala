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

package org.apache.spark.blaze.mpi

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{ContextCleaner, ExecutorAllocationManager, Heartbeater, SparkConf, SparkEnv, SparkStatusTracker}
import org.apache.spark.internal.plugin.PluginContainer
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.{DAGScheduler, EventLoggingListener, LiveListenerBus, SchedulerBackend, TaskScheduler}
import org.apache.spark.shuffle.api.ShuffleDriverComponents
import org.apache.spark.status.AppStatusStore
import org.apache.spark.ui.{ConsoleProgressBar, SparkUI}
import org.apache.spark.util.logging.DriverLogger

import java.net.URI

class MPIContext {

  def stop(): Unit = {
    // TODO: stop MPIContext
  }

  /* ------------------------------------------------------------------------------------- *
 | Private variables. These variables keep the internal state of the context, and are    |
 | not accessible by the outside world. They're mutable since we want to initialize all  |
 | of them to some neutral value ahead of time, so that calling "stop()" while the       |
 | constructor is still running is safe.                                                 |
 * ------------------------------------------------------------------------------------- */

  private var _conf: SparkConf = _
  private var _eventLogDir: Option[URI] = None
  private var _eventLogCodec: Option[String] = None
  private var _listenerBus: LiveListenerBus = _
  private var _env: SparkEnv = _
  private var _statusTracker: SparkStatusTracker = _
  private var _progressBar: Option[ConsoleProgressBar] = None
  private var _ui: Option[SparkUI] = None
  private var _hadoopConfiguration: Configuration = _
  private var _executorMemory: Int = _
  private var _schedulerBackend: SchedulerBackend = _
  private var _taskScheduler: TaskScheduler = _
  private var _heartbeatReceiver: RpcEndpointRef = _
  @volatile private var _dagScheduler: DAGScheduler = _
  private var _applicationId: String = _
  private var _applicationAttemptId: Option[String] = None
  private var _eventLogger: Option[EventLoggingListener] = None
  private var _driverLogger: Option[DriverLogger] = None
  private var _executorAllocationManager: Option[ExecutorAllocationManager] = None
  private var _cleaner: Option[ContextCleaner] = None
  private var _listenerBusStarted: Boolean = false
  private var _jars: Seq[String] = _
  private var _files: Seq[String] = _
  private var _shutdownHookRef: AnyRef = _
  private var _statusStore: AppStatusStore = _
  private var _heartbeater: Heartbeater = _
  private var _resources: scala.collection.immutable.Map[String, ResourceInformation] = _
  private var _shuffleDriverComponents: ShuffleDriverComponents = _
  private var _plugins: Option[PluginContainer] = None


}
