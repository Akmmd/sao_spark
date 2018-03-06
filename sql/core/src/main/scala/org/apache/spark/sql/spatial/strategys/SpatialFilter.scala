package org.apache.spark.sql.spatial.trategys

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.spatial.execution.SpatialFilterExec

object SpatialFilter extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case logical.Filter(condition,child) =>
        SpatialFilterExec(condition,planLater(child),plan) :: Nil
      case _ => Nil
    }
  }
}