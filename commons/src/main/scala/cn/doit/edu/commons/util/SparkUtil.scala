package cn.doit.edu.commons.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author charley
  * @create 2020-12-20-20
  */
object SparkUtil {
  def getSparkSession(appName:String = "app",master:String = "local[*]",confMap:Map[String,String] = Map.empty):SparkSession = {
    val conf = new SparkConf()
    conf.setAll(confMap)
    SparkSession.builder().appName(appName).master(master).config(conf).getOrCreate()

  }
}
