package org.apache.spark.sql.catalyst.spatial

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import java.io._
import org.apache.spark.sql.catalyst.spatial.shapes._
import org.apache.spark.sql.catalyst.spatial.util.KryoSerializer

object ShapeSerializer {
  private[sql] val kryo = new Kryo()   //sql指在sql包下面是共有访问的，其他地方是私有的
  kryo.register(classOf[Shape],new KryoSerializer)  //注册
  kryo.register(classOf[Point],new KryoSerializer)
  kryo.addDefaultSerializer(classOf[Shape],new KryoSerializer)  //默认序列化方法
  kryo.setReferences(false)   //不允许引用

  def deserialize(data: Array[Byte]): Shape = {   //反序列号
    val in = new ByteArrayInputStream(data)
    val input = new Input(in)
    val res = kryo.readObject(input,classOf[Shape])
    input.close()
    res
  }

  def serialize(shape: Shape): Array[Byte] = {   //序列化
    val out = new ByteArrayOutputStream()
    val output = new Output(out)
    kryo.writeObject(output,shape)
    output.close()
    out.toByteArray
  }
}