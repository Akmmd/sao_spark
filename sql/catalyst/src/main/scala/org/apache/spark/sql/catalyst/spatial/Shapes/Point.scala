package org.apache.spark.sql.catalyst.spatial.shapes

import org.apache.spark.sql.catalyst.spatial.ShapeType
import org.apache.spark.sql.types.SQLUserDefinedType

@SQLUserDefinedType(udt = classOf[ShapeType])
case class Point(coord : Array[Double]) extends Shape {
  override val dimnsions: Int = coord.length   //维数

  override def minDist(other: Shape): Double = {   //不同类型不同方法
    other match{
      case p : Point => minDist(p)
      case mbr : MBR => mbr.minDist(this)
    }
  }

  def minDist(other: Point): Double = {   //2点之间距离
    require(coord.length == other.coord.length)
    var ans = 0.0
    for(i <- coord.indices)
      ans += (coord(i) - other.coord(i)) * (coord(i) - other.coord(i))
    Math.sqrt(ans)
  }

  override def intersects(other: Shape): Boolean = {   //判断是否相交
    other match{
      case p : Point => p == this
      case mbr : MBR => mbr.contains(this)
    }
  }

  def ==(other: Point): Boolean = {   //重写判断坐标是否相等
    other match{
      case p : Point =>
        //判断维数
        if(p.coord.length != coord.length) false   //coord是this
        else{
          //判断值
          for(i <- coord.indices){
            if(coord(i) != p.coord(i)) return false
          }
          true
        }
      case _ => false
    }
  }

  def <=(other: Point): Boolean = {
    require(coord.length == other.coord.length)
    for(i <- coord.indices){
      if(coord(i) > other.coord(i)) return false
    }
    true
  }
}