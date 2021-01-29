package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author charley
  * @create 2021-01-17-17
  */
case class Stu(id:Int,name:String,age:Int,city:String,score:Double)
object Demo01_CreateDF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      .getOrCreate()
    val rdd: RDD[String] = spark.sparkContext.textFile("dataware/stu.txt")
    //将rdd转成DataFrame
    val rddStu: RDD[Stu] = rdd.map(line=>{
      line.split(",")
    }).map(arr=>Stu(arr(0).toInt,arr(1),arr(2).toInt,arr(3),arr(4).toDouble))
    //然后可以将这种装着case class类型数据的RDD直接转成dataframe]
    val df = spark.createDataFrame(rddStu)
    df.printSchema()
    df.show()
    spark.close()
  }
}
