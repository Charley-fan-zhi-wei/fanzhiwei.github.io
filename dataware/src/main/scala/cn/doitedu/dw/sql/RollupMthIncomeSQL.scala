package cn.doitedu.dw.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author charley
  * @create 2021-01-19-19
  */
object RollupMthIncomeSQL {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[*]").getOrCreate()
    val orders: DataFrame = spark.read.option("header","true").option("inferSchema","true").csv("dataware/order.csv")
    orders.createTempView("v_orders")
    spark.sql(
      """
        |select
        |	sid,
        |	mth,
        |	mth_sales,
        |	sum(mth_sales) over(partition by sid order by mth rows between unbounded preceding and current row) rollup_sales
        |from(
        |select
        |	sid,
        |	concat_ws("-",year(dt),month(dt)) mth,
        |	sum(money) mth_sales
        |from v_orders
        |group by sid,mth
        |)
      """.stripMargin).show()
spark.stop()
}
}
