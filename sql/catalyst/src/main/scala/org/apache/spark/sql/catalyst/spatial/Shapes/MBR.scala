package org.apache.spark.sql.catalyst.spatial.shapes

import org.apache.spark.sql.catalyst.spatial.ShapeType
import org.apache.spark.sql.types.{SQLUserDefinedType, UserDefinedType}

/*
 *   Created by Sao on 2018/3/4
 */

@SQLUserDefinedType(udt = classOf[ShapeType])
case class MBR(low : Point,high : Point) extends Shape {
  require(low.dimnsions == high.dimnsions)
  require(low <= high)
  override val dimnsions: Int = high.dimnsions

  override def minDist(other: Shape): Double = {
    other match {
      case p: Point => minDist(p)
      case mbr: MBR => mbr.minDist(this)
    }
  }

  def minDist(p: Point) : Double ={   //求点到mbr的最近距离
    require(low.coord.length == p.coord.length)
    var ans = 0.0
    for(i <- p.coord.indices){
      if(p.coord(i) < low.coord(i)){
        ans += (low.coord(i) - p.coord(i)) * (low.coord(i) - p.coord(i))
      } else if(p.coord(i) > high.coord(i)){
        ans += (p.coord(i) - high.coord(i)) * (p.coord(i) - high.coord(i))
      }
    }
    Math.sqrt(ans)
  }

  override def intersects(other: Shape): Boolean = {
    other match{
      case p: Point => p == this
      case mbr: MBR => intersects(mbr)
    }
  }

  def intersects(other: MBR): Boolean = {
    require(high.coord.length == other.high.coord.length)
    for(i <- low.coord.indices){
      if(low.coord(i) > other.high.coord(i) || high.coord(i) < other.low.coord(i))
        return false
    }
    true
  }

  def contains(point: Shape): Boolean = {   //判断点是否在MBR中
    point match {
      case p: Point =>
        for(i <- p.coord.indices){   //从数组coord取值
          if(p.coord(i) < low.coord(i) || p.coord(i) > high.coord(i))
            return false
        }
        true
      case _ => false
    }
  }

  override def toString: String = "MBR(" + low.toString + "," + high.toString + ")"
}