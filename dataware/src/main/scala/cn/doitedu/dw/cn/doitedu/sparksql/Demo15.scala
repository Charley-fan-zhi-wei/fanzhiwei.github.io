package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.spark.sql.Dataset

/**
  * @author charley
  * @create 2021-01-18-18
  */
object Demo15 {
  def main(args: Array[String]): Unit = {
  val spark = SparkUtil.getSparkSession()
  val lines: Dataset[String] = spark.read.textFile("dataware/words.txt")
  import spark.implicits._
  val words = lines.flatMap(_.split(" "))
  words.printSchema()
  words.show()
  words.createTempView("words")
  spark.sql("select value as word,count(1) as counts from words group by value order by counts desc").show()
  spark.close()
}
}
