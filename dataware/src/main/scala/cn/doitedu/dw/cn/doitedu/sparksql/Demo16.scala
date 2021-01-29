package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.spark.sql.Dataset

/**
  * @author charley
  * @create 2021-01-18-18
  */
object Demo16 {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession()
    val lines: Dataset[String] = spark.read.textFile("dataware/words.txt")
    lines.createTempView("v_lines")
    import spark.implicits._
    spark.sql(
      """
        |select word,count(1) as counts from
        |(select explode(words) as word from
        |   (select split(value,' ') as words from v_lines)
        |)
        |group by word order by counts desc
      """.stripMargin).show()
    spark.close()
  }
}
