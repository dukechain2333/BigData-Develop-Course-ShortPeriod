package Exp1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Question4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("question4").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    val df1 = spark.read.json("Data/191650126_钱泽昊_原始数据_1.json")
    infoReader("Amazon", df1)
    val df2 = spark.read.json("Data/191650126_钱泽昊_原始数据_2.json")
    infoReader("JingDong", df2)
    val df3 = spark.read.json("Data/191650126_钱泽昊_原始数据_3.json")
    infoReader("DangDang", df3)
    val df_all = df1.union(df2).union(df3)
    println("The books and their average price:")
    df_all.groupBy("name").agg("price"->"mean").show()

  }

  def infoReader(name: String, df: DataFrame): Unit = {
    println("This is the info of:" + name)
    df.createOrReplaceTempView("price_info")
    df.sparkSession.sql("SELECT name,ISBN,price FROM price_info").show()
  }
}
