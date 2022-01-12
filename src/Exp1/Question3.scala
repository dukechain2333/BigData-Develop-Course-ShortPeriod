package Exp1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Question3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("question3").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val lines1 = sc.textFile("Data/191650126_钱泽昊_原始数据_1.json")
    infoReader("Amazon", lines1)
    val lines2 = sc.textFile("Data/191650126_钱泽昊_原始数据_2.json")
    infoReader("JingDong", lines2)
    val lines3 = sc.textFile("Data/191650126_钱泽昊_原始数据_3.json")
    infoReader("DangDang", lines3)
    val lines = lines1 ++ lines2 ++ lines3
    var data = lines.map(x => {
      val splitData = x.split(',')
      val isbn = splitData(1).split(':')(1)
      val name = splitData(0).split(':')(1)
      val price = splitData(2).split(": \"")(1).dropRight(1).toDouble
      (name, price)
    })
    val result = data.groupByKey().map(x => {
      val total = x._2.reduce(_ + _)
      (x._1, total / x._2.size)
    }).foreach(println)
  }

  def infoReader(name: String, data: RDD[String]) {
    println("This is the info of:" + name)
    data.foreach(x => {
      val splitData = x.split(',')
      val isbn = splitData(1).split(':')(1).substring(1)
      val bookName = splitData(0).split(": \"")(1).dropRight(1)
      val price = splitData(2).split(": \"")(1).dropRight(1).toDouble
      println(bookName + "(" + isbn + ")'s price is: " + price)
    })
    println("-" * 30)
  }
}


