package cn.doitedu.dw.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author charley
  * @create 2021-01-19-19
  */
object UserContinuedLoginSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[*]").getOrCreate()
    //从csv文件读取数据
    val df: DataFrame = spark.read.option("header","true").csv("dataware/access.csv")
    //注册成视图
    df.createTempView("v_access_log")
    //写SQL
    spark.sql(
      """
        |select
        |	uid,
        |	min(dt) as start_date,
        |	max(dt) as end_date,
        |	count(1) as counts
        |from
        |(
        |		select
        |		uid,
        |		dt,
        |		date_sub(dt,rn) as dif
        |		from
        |		(
        |			select
        |			uid,
        |			dt,
        |			row_number() over(partition by uid order by dt asc) as rn
        |			from v_access_log
        |		) o
        |)
        |group by uid,dif
        |having counts>=3
      """.stripMargin).show()
    spark.close()
  }
}
