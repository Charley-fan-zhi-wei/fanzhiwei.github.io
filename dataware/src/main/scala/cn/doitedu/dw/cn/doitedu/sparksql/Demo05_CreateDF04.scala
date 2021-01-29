package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author charley
  * @create 2021-01-17-17
  */
object Demo04_CreateDF03 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      .getOrCreate()
    val rdd: RDD[String] = spark.sparkContext.textFile("dataware/stu.txt")
    //1.将RDD[String]变成RDD[JavaStu]
    val rddScalaStu: RDD[ScalaStu] = rdd.map(line => {
      val arr = line.split(",")
      new ScalaStu(arr(0).toInt, arr(1), arr(2).toInt, arr(3), arr(4).toDouble)
    })
    val df = spark.createDataFrame(rddScalaStu,classOf[ScalaStu])
    df.printSchema()
    df.show()
  }
}
