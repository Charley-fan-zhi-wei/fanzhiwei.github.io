package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @author charley
  * @create 2021-01-17-17
  */
object Demo05_CreateDF05 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      .getOrCreate()
    val rdd: RDD[String] = spark.sparkContext.textFile("dataware/stu.txt")
    //1.将RDD[String]变成RDD[ROW]
    val rdd2: RDD[Row] = rdd.map(line=>{
      val arr = line.split(",")
      Row(arr(0).toInt,arr(1),arr(2).toInt,arr(3),arr(4).toDouble)
    })
    val schema: StructType = new StructType(Array(
      new StructField("id", DataTypes.IntegerType, true),
      new StructField("name", DataTypes.StringType),
      new StructField("age", DataTypes.IntegerType),
      new StructField("city", DataTypes.StringType),
      new StructField("scores", DataTypes.DoubleType)
    ))
    val df = spark.createDataFrame(rdd2,schema)
    //df.printSchema()
    df.where("scores>87").select("name","age","scores").show()
    spark.close()
  }
}
