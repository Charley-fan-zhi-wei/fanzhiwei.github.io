package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * @author charley
  * @create 2021-01-18-18
  */
object Demo07_CreateDF_CSV {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession()
    val schema = new StructType(Array(
      StructField("id",DataTypes.IntegerType),
      StructField("name",DataTypes.StringType),
      StructField("age",DataTypes.IntegerType),
      StructField("city",DataTypes.StringType),
      StructField("score",DataTypes.DoubleType)
    ))
    val df = spark.read.schema(schema).csv("dataware/stu.csv")
    df.printSchema()
    df.show()
    val df2 = spark.read.option("inferSchema","true").csv("dataware/stu.csv")
    df2.printSchema()
    spark.close()
  }
}
