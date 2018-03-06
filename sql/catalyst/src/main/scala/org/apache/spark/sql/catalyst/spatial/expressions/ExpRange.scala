package org.apache.spark.sql.catalyst.spatial.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.spatial.util.ShapeUtil
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.spatial.shapes.{MBR, Point}

/*
 *   Created by Sao on 2018/3/5
 */

case class ExpRange(shape: Expression, low: Expression, high: Expression) extends Predicate with CodegenFallback {
  override def nullable: Boolean = false

  override def toString: String = s" **($shape) IN Rectangle ($low) - ($high)**  "

  override def children: Seq[Expression] = Seq(shape,low,high)

  override def eval(input: InternalRow): Any = {
    val data = ShapeUtil.getShape(shape,input)
    val low_point = low.asInstanceOf[Literal].value.asInstanceOf[Point]
    val high_point = high.asInstanceOf[Literal].value.asInstanceOf[Point]
    require(data.dimnsions == low_point.dimnsions && low_point.dimnsions == high_point.dimnsions)
    val mbr = new MBR(low_point,high_point)
    mbr.contains(data)
  }
}