package cn.doitedu.dw.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @author charley
  * @create 2021-01-19-19
  */
object RollupMthIncomeRDD {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[*]").getOrCreate()
    val orders: DataFrame = spark.read.option("header","true").csv("dataware/order.csv")
    //取出Dataframe中的RDD
    val lines: RDD[Row] = orders.rdd
    //根据sid和月份进行聚合
    val reduced: RDD[((String, String), Double)] = lines.map(row=>{
      val sid = row.getString(0)
      val dateStr = row.getString(1)
      val month = dateStr.substring(0,7)
      val money = row.getString(2).toDouble
      //将shopid和month合起来当成key
      ((sid,month),money)
    }).reduceByKey(_+_)
    //根据sid分组排序
    val result: RDD[(String, String, Double, Double)] = reduced.groupBy(_._1._1).mapValues(it=>{
      //将迭代器中的数据toList放入到内存
      //按照月份排序
      val sorted: List[((String, String), Double)] = it.toList.sortBy(_._1._2)
      var rollup = 0.0
      //迭代数据
      sorted.map(t=>{
        val sid = t._1._1
        val mth = t._1._2
        val mth_sales = t._2
        rollup += mth_sales
        (mth,mth_sales,rollup)
      })
    }).flatMapValues(lst => lst).map(t=>(t._1,t._2._1,t._2._2,t._2._3))
    result.foreach(println _)
    spark.close()
  }
}
