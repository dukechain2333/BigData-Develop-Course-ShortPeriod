package Exp4

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Question1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Question1").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().appName("Question1").getOrCreate()
    val lines = sc.textFile("Data/employee.txt")
    val schemaString = "id name age"
    val fields = schemaString.split(" ").map(fieldname => StructField(fieldname, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = lines.map(_.split(",")).map(elements => Row(elements(0), elements(1), elements(2)))
    val employeeDF = spark.createDataFrame(rowRDD, schema)
    employeeDF.collect().foreach(x => {
      println("id:" + x(0) + " name:" + x(1) + " age:" + x(2))
    })
  }
}
