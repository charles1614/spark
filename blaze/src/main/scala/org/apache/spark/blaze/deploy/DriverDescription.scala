
package org.apache.spark.blaze.deploy

import org.apache.spark.deploy.Command
import org.apache.spark.resource.ResourceRequirement

private[deploy] case class DriverDescription(
                                              jarUrl: String,
                                              mem: Int,
                                              cores: Int,
                                              supervise: Boolean,
                                              command: Command,
                                              resourceReqs: Seq[ResourceRequirement] = Seq.empty) {

  override def toString: String = s"DriverDescription (${command.mainClass})"
}
