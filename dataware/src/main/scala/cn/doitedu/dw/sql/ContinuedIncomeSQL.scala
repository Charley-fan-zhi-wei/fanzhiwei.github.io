package cn.doitedu.dw.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author charley
  * @create 2021-01-19-19
  */
object ContinuedIncomeSQL {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[*]").getOrCreate()
    val orders: DataFrame = spark.read.option("header","true").option("inferSchema","true").csv("dataware/order.csv")
    orders.createTempView("v_orders")
    spark.sql(
      """
      |select
      |	sid,
      |	min(dt) as start_date,
      |	max(dt) as end_date,
      |	count(1) as counts,
      |	sum(daily_sales) as sum_sales
      |from(
      |	select
      |		sid,
      |		dt,
      |		daily_sales,
      |		date_sub(dt,rn) as dif
      |	from(
      |			select
      |			sid,
      |			dt,
      |			daily_sales,
      |			row_number() over(partition by sid order by dt asc) as rn
      |			from(
      |				select
      |				sid,
      |				dt,
      |				sum(money) as daily_sales
      |				from v_orders
      |				group by sid,dt
      |				)
      |		) as t1
      |) as t2
      |group by sid,dif
      |having counts >= 3
      """.stripMargin).show()
    spark.stop()
  }
}
