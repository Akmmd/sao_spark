package org.apache.spark.sql.catalyst.spatial.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.spatial.shapes.Point
import org.apache.spark.sql.catalyst.spatial.ShapeType

/*
 *   Created by Sao on 2018/3/5
 */

case class PointWrapper(exps: Seq[Expression]) extends Expression with CodegenFallback {
  override def nullable: Boolean = false   //参数不允许为空

  override def dataType: DataType = ShapeType

  override def children: Seq[Expression] = exps

  override def eval(input: InternalRow): Any = {   //将Row数组变成一个Point
    val coords = exps.map(_.eval(input).asInstanceOf[Double]).toArray
    Point(coords)
  }

}