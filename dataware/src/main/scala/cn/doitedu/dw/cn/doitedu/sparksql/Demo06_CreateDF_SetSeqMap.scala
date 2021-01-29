package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author charley
  * @create 2021-01-18-18
  */
object Demo06_CreateDF_SetSeqMap {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
    val seq1 = Seq(1,2,3,4)
    val seq2 = Seq(11,22,33)
    val rdd: RDD[Seq[Int]] = spark.sparkContext.parallelize(List(seq1,seq2))
    import spark.implicits._
    val df: DataFrame = rdd.toDF()
    df.printSchema()
    df.show()
    df.selectExpr("value[0]","size(value)").show()

    val set1 = Set("a","b")
    val set2 = Set("a","b","c")
    val rdd2: RDD[Set[String]] = spark.sparkContext.parallelize(List(set1,set2))
    val df2 = rdd2.toDF()
    df2.printSchema()
    df2.show()

    val map1 = Map("father"->"mayun","mother"->"tangyan")
    val map2 = Map("father"->"huateng","mother"->"yifei","brother"->"sicong")
    val rdd3 = spark.sparkContext.parallelize(List(map1,map2))
    val df3 = rdd3.toDF("jiaren")
    df3.printSchema()
    df3.show()
    df3.selectExpr("jiaren['mother']","size(jiaren)","map_keys(jiaren)","map_values(jiaren)").show()
    spark.close()
  }
}
