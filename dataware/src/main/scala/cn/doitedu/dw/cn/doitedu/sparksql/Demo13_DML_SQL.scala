package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.spark.sql.DataFrame

/**
  * @author charley
  * @create 2021-01-18-18
  */
object Demo13_DML_SQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession()
    val df: DataFrame = spark.read.option("header","true").csv("dataware/stu2.csv")
    df.createTempView("view1")
    spark.sql("select * from view1").show()
    spark.sql("select * from view1 order by score desc limit 2").show()
    df.createGlobalTempView("view2")
    spark.sql("select * from global_temp.view2").show()
    spark.close()
  }
}
