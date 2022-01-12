package Exp4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Question3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Question3").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().appName("Question3").getOrCreate()
    val classData = Seq(
      Row("class1", "LiLei"),
      Row("class1", "HanMeiMei"),
      Row("class2", "DaChui"),
      Row("class2", "RuHua")
    )
    val scoreData = Seq(
      Row("LiLei", 76),
      Row("HanMeiMei", 80),
      Row("DaChui", 70),
      Row("RuHua", 60)
    )
    val classSchema = StructType(
      List(
        StructField("class", StringType, true),
        StructField("name", StringType, true)
      )
    )
    val scoreSchema = StructType(
      List(
        StructField("name", StringType, true),
        StructField("score", IntegerType, true)
      )
    )
    val classDF = spark.createDataFrame(sc.makeRDD(classData), classSchema)
    val scoreDF = spark.createDataFrame(sc.makeRDD(scoreData), scoreSchema)
    val joinDF = classDF.join(scoreDF, classDF("name") === scoreDF("name"))
    val aggDF = joinDF.groupBy("class").agg(mean("score"))
    aggDF.filter(aggDF("avg(score)") >= 75).show()
  }
}
