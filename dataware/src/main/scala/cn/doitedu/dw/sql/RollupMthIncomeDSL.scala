package cn.doitedu.dw.sql

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author charley
  * @create 2021-01-19-19
  */
object RollupMthIncomeDSL {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[*]").getOrCreate()
    val orders: DataFrame = spark.read.option("header","true").option("inferSchema","true").csv("dataware/order.csv")
    //1.按照sid,月份进行聚合，sum(money)
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val res: DataFrame = orders.groupBy($"sid",date_format($"dt","yyyy-MM") as "mth").agg(sum("money") as "mth_sales")
        .select(col("sid"),'mth,$"mth_sales",sum("mth_sales") over(Window.partitionBy("sid").orderBy("mth").rowsBetween(Window.unboundedPreceding,Window.currentRow)) as "rollup_sales")
    res.show()
    spark.close()
  }
}
