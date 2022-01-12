package Exp4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object Question2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Question2").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().appName("Question2").getOrCreate()
    val students = sc.makeRDD(List(("LiLei", 18, 87), ("HanMeiMei", 16, 77), ("DaChui", 16, 66), ("Jim", 18, 77), ("RuHua", 18, 50)))
    val schema = StructType(
      List(
        StructField("name",StringType,true),
        StructField("age",IntegerType,true),
        StructField("score",IntegerType,true)
      )
    )
    val rowRDD = students.map(elements => Row(elements._1, elements._2, elements._3))
    val studentDF = spark.createDataFrame(rowRDD, schema)
    studentDF.sort(desc("score")).show(3)
  }
}
