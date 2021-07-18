
package org.apache.spark.blaze.deploy

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.resource.{ResourceAllocation, ResourceID, ResourceInformation, ResourceRequirement}
import org.apache.spark.util.Utils
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.{DefaultFormats, Extraction}

import java.io.File
import java.nio.file.Files
import scala.collection.mutable
import scala.util.control.NonFatal

private[spark] object StandaloneResourceUtils extends Logging {

  /**
   * A mutable resource information which provides more efficient modification on addresses.
   */
  private[spark] case class MutableResourceInfo(name: String, addresses: mutable.HashSet[String]) {

    def +(other: MutableResourceInfo): this.type = {
      assert(name == other.name, s"Inconsistent resource name, expected $name, " +
        s"but got ${other.name}")
      other.addresses.foreach(this.addresses.add)
      this
    }

    def +(other: ResourceInformation): this.type = {
      assert(name == other.name, s"Inconsistent resource name, expected $name, " +
        s"but got ${other.name}")
      other.addresses.foreach(this.addresses.add)
      this
    }

    def -(other: ResourceInformation): this.type = {
      assert(name == other.name, s"Inconsistent resource name, expected $name, " +
        s"but got ${other.name}")
      other.addresses.foreach(this.addresses.remove)
      this
    }

    def toResourceInformation: ResourceInformation = {
      new ResourceInformation(name, addresses.toArray)
    }
  }

  /**
   * Resource allocation used in Standalone only, which tracks assignments with
   * worker/driver(client only) pid.
   */
  case class StandaloneResourceAllocation(pid: Int, allocations: Seq[ResourceAllocation]) {
    // convert allocations to a resource information map
    def toResourceInformationMap: Map[String, ResourceInformation] = {
      allocations.map { allocation =>
        allocation.id.resourceName -> allocation.toResourceInformation
      }.toMap
    }
  }

  /**
   * Save the allocated resources of driver(cluster only) or executor into a JSON formatted
   * resources file. Used in Standalone only.
   *
   * @param componentName spark.driver / spark.executor
   * @param resources     allocated resources for driver(cluster only) or executor
   * @param dir           the target directory used to place the resources file
   * @return None if resources is empty or Some(file) which represents the resources file
   */
  def prepareResourcesFile(
                            componentName: String,
                            resources: Map[String, ResourceInformation],
                            dir: File): Option[File] = {
    if (resources.isEmpty) {
      return None
    }

    val compShortName = componentName.substring(componentName.lastIndexOf(".") + 1)
    val tmpFile = Utils.tempFileWith(dir)
    val allocations = resources.map { case (rName, rInfo) =>
      ResourceAllocation(new ResourceID(componentName, rName), rInfo.addresses)
    }.toSeq
    try {
      writeResourceAllocationJson(componentName, allocations, tmpFile)
    } catch {
      case NonFatal(e) =>
        val errMsg = s"Exception threw while preparing resource file for $compShortName"
        logError(errMsg, e)
        throw new SparkException(errMsg, e)
    }
    val resourcesFile = File.createTempFile(s"resource-$compShortName-", ".json", dir)
    tmpFile.renameTo(resourcesFile)
    Some(resourcesFile)
  }

  private def writeResourceAllocationJson[T](
                                              componentName: String,
                                              allocations: Seq[T],
                                              jsonFile: File): Unit = {
    implicit val formats = DefaultFormats
    val allocationJson = Extraction.decompose(allocations)
    Files.write(jsonFile.toPath, compact(render(allocationJson)).getBytes())
  }

  def toMutable(immutableResources: Map[String, ResourceInformation])
  : Map[String, MutableResourceInfo] = {
    immutableResources.map { case (rName, rInfo) =>
      val mutableAddress = new mutable.HashSet[String]()
      mutableAddress ++= rInfo.addresses
      rName -> MutableResourceInfo(rInfo.name, mutableAddress)
    }
  }

  // used for UI
  def formatResourcesDetails(
                              usedInfo: Map[String, ResourceInformation],
                              freeInfo: Map[String, ResourceInformation]): String = {
    usedInfo.map { case (rName, rInfo) =>
      val used = rInfo.addresses.mkString("[", ", ", "]")
      val free = freeInfo(rName).addresses.mkString("[", ", ", "]")
      s"$rName: Free: $free / Used: $used"
    }.mkString(", ")
  }

  // used for UI
  def formatResourcesAddresses(resources: Map[String, ResourceInformation]): String = {
    resources.map { case (rName, rInfo) =>
      s"$rName: ${rInfo.addresses.mkString("[", ", ", "]")}"
    }.mkString(", ")
  }

  // used for UI
  def formatResourcesUsed(
                           resourcesTotal: Map[String, Int],
                           resourcesUsed: Map[String, Int]): String = {
    resourcesTotal.map { case (rName, totalSize) =>
      val used = resourcesUsed(rName)
      val total = totalSize
      s"$used / $total $rName"
    }.mkString(", ")
  }

  // used for UI
  def formatResourceRequirements(requirements: Seq[ResourceRequirement]): String = {
    requirements.map(req => s"${req.amount} ${req.resourceName}").mkString(", ")
  }
}
