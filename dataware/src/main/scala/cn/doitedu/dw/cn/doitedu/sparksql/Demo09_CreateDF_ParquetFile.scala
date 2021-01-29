package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.spark.sql.SparkSession

/**
  * @author charley
  * @create 2021-01-18-18
  */
object Demo09_CreateDF_ParquetFile {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession()
    val df = spark.read.parquet("dataware/parquet")
    df.printSchema()
    df.show()
    spark.close()
  }
}
