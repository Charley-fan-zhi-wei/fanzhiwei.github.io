package cn.doitedu.dw.cn.doitedu.sparksql

import java.util.Properties

import scala.tools.cmd.Property

/**
  * @author charley
  * @create 2021-01-18-18
  */
object Demo10_CreateDF_JDBC {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession()
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","hadoop")
    val df = spark.read.jdbc("jdbc:mysql://localhost:3306/demo","student",props)
    df.printSchema()
    df.show()
    spark.close()
  }
}
