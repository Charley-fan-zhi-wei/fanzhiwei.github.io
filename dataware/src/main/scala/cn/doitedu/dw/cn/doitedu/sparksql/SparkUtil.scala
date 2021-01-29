package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @author charley
  * @create 2021-01-18-18
  */
object SparkUtil {
  def getSparkSession(appName:String = "demo",master:String="local[*]"):SparkSession ={
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName(appName).master(master).getOrCreate()
    spark
  }
}
