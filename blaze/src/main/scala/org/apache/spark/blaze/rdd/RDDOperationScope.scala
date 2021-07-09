// scalastyle:off

package org.apache.spark.blaze.rdd

import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonInclude, JsonPropertyOrder}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.SparkContext
import org.apache.spark.blaze.mpi.MPIContext
import org.apache.spark.internal.Logging


@JsonInclude(Include.NON_ABSENT)
@JsonPropertyOrder(Array("id", "name", "parent"))
private[spark] class RDDOperationScope(
                                        val name: String,
                                        val parent: Option[RDDOperationScope] = None,
                                        val id: String = RDDOperationScope.nextScopeId().toString
                                      ) extends Logging {

  def toJson(): String = {
    RDDOperationScope.jsonMapper.writeValueAsString(this)
  }
}

private[spark] object RDDOperationScope extends Logging {

  private val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val scopeCounter = new AtomicInteger(0)

  def nextScopeId(): Int = {
    scopeCounter.getAndIncrement()
  }

  def fromJson(s: String): RDDOperationScope = {
    jsonMapper.readValue(s, classOf[RDDOperationScope])
  }

  // TODO: merge withScope sc and mc
  private[spark] def withScope[T](
                                   mc: MPIContext,
                                   allowNesting: Boolean = false)(body: => T): T = {
    val ourMethodName = "withScope"
    val callerMethodName = Thread.currentThread.getStackTrace()
      .dropWhile(_.getMethodName != ourMethodName)
      .find(_.getMethodName != ourMethodName)
      .map(_.getMethodName)
      .getOrElse {
        // Log a warning just in case, but this should almost certainly never happen
        logWarning("No valid method name for this RDD operation scope!")
        "N/A"
      }
    withScope[T](mc, callerMethodName, allowNesting, ignoreParent = false)(body)
  }

  // mpi
  private[spark] def withScope[T](
                                   mc: MPIContext,
                                   name: String,
                                   allowNesting: Boolean,
                                   ignoreParent: Boolean)(body: => T): T = {
    // Save the old scope to restore it later
    val scopeKey = SparkContext.RDD_SCOPE_KEY
    val noOverrideKey = SparkContext.RDD_SCOPE_NO_OVERRIDE_KEY
    val oldScopeJson = mc.getLocalProperty(scopeKey)
    val oldScope = Option(oldScopeJson).map(RDDOperationScope.fromJson)
    val oldNoOverride = mc.getLocalProperty(noOverrideKey)
    try {
      if (ignoreParent) {
        // Ignore all parent settings and scopes and start afresh with our own root scope
        mc.setLocalProperty(scopeKey, new RDDOperationScope(name).toJson)
      } else if (mc.getLocalProperty(noOverrideKey) == null) {
        // Otherwise, set the scope only if the higher level caller allows us to do so
        mc.setLocalProperty(scopeKey, new RDDOperationScope(name, oldScope).toJson)
      }
      // Optionally disallow the child body to override our scope
      if (!allowNesting) {
        mc.setLocalProperty(noOverrideKey, "true")
      }
      body
    } finally {
      // Remember to restore any state that was modified before exiting
      mc.setLocalProperty(scopeKey, oldScopeJson)
      mc.setLocalProperty(noOverrideKey, oldNoOverride)
    }
  }
}
