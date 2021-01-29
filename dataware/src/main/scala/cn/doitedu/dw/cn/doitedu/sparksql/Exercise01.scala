package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author charley
  * @create 2021-01-18-18
  * 利用spark sql对电影评分数据进行分析
  */
object Exercise01 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val df: DataFrame = spark.read.json("dataware/mv.json")
    val df2 = df.select("movie","rate","timeStamp","uid").where("rate is not null")
    val mvView = df2.createTempView("mv")
    val res: DataFrame = spark.sql("select movie,avg(cast(rate as int)) as avg_rate from mv group by movie")
    val res2: DataFrame = spark.sql("select movie,sum(cast(rate as int)) as avg_rate from mv group by movie")
    var res3 = spark.sql(
      """
        |select
        |movie,
        |rate,
        |timeStamp,
        |uid
        |from
        |(
        |select
        |movie,
        |rate,
        |timeStamp,
        |uid,
        |row_number() over(partition by uid order by cast(rate as int) desc) as rn
        |from mv
        | ) o
        | where rn<= 2
      """.stripMargin)
    val res4 = spark.sql(
      """
        |select
        |movie,
        |count(1) as ts
        |from mv
        |group by movie
        |order by ts desc
        |limit 50
      """.stripMargin)
    df.printSchema()
    df.show()
    res.show()
    res2.show()
    res3.show()
    res4.show()
    spark.close()
  }
}
