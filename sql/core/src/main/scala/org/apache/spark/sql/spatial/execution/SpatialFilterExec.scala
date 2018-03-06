package org.apache.spark.sql.spatial.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PredicateHelper, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.spatial.expressions.ExpRange

case class SpatialFilterExec(condition: Expression, child: SparkPlan, lp: LogicalPlan) extends SparkPlan with PredicateHelper {
  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] = {
    val root_rdd = child.execute()
    condition match {
      case ExpRange(shape, low, high) =>
        root_rdd.mapPartitions(iter => iter.filter(newPredicate(condition,child.output).eval(_)))
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning
}