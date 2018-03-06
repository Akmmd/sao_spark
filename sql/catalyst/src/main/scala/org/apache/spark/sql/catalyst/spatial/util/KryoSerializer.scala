package org.apache.spark.sql.catalyst.spatial.util

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.sql.catalyst.spatial.shapes.{Shape, Point}

/*
 *   Created by Sao on 2018/3/5
 */

class KryoSerializer extends Serializer[Shape]{
  private def getTypeInt(o: Shape): Short = {
    o match{
      case p: Point => 0
    }
  }

  override def write(kryo: Kryo, output: Output, shape: Shape): Unit ={   //=>byte
    output.writeShort(getTypeInt(shape))   //先写入type
    shape match {
      case p: Point =>
        output.writeInt(p.dimnsions,true)   //先写入维度
        p.coord.foreach(output.writeDouble) //写入坐标
    }
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[Shape]): Shape = {
    val tp = input.readShort()
    if(tp == 0){
      val dim = input.readInt(true)   //读维度
      val coords = Array.ofDim[Double](dim)   //建立数组
      for(i <- 0 until dim) coords(i) = input.readDouble()   //赋值，until(0 -> dim-1)
      Point(coords)   //建立一个Point
    }else null
  }
}