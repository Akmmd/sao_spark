package org.apache.spark.examples.sql.spatial

import org.apache.spark.sql.SparkSession

/*
 *   Created by Sao on 2018/3/6
 */
object RangeExample {


  case class PointData(x: Double, y: Double, z: Double, other: String)

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("SparkSession")
      .getOrCreate()

    import sparkSession.implicits._
    val caseClassDS = Seq(PointData(1.0, 1.0, 3.0, "1"), PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"), PointData(3.0, 3.0, 3.0, "5"), PointData(4.0, 4.0, 3.0, "6")).toDS()
    caseClassDS.range(Array("x", "y"), Array(0.0, 0.0), Array(3.0, 3.0)).show()
  }
}
