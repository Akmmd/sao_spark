package org.apache.spark.sql.catalyst.spatial.util

import org.apache.spark.sql.types._

/*
 *   Created by Sao on 2018/3/5
 */

object NumberUtil {   //判断是否是数字
  def isInteger(x: DataType): Boolean = {
    x match {
      case IntegerType => true
      case LongType => true
      case ShortType => true
      case ByteType => true
      case _ => false
    }
  }
}