package org.apache.spark.sql.catalyst.spatial.shapes

import org.apache.spark.sql.catalyst.spatial.ShapeType
import org.apache.spark.sql.types.SQLUserDefinedType

@SQLUserDefinedType(udt = classOf[ShapeType])
abstract class Shape extends Serializable {
  val dimnsions : Int

  def minDist(other: Shape): Double

  def intersects(other: Shape): Boolean
}