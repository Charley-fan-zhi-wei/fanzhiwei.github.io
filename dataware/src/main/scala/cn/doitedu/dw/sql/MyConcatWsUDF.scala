package cn.doitedu.dw.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author charley
  * @create 2021-01-19-19
  * UDF输入一行，返回一行
  */
object MyConcatWsUDF {
  def main(args: Array[String]): Unit = {
    val spark =  SparkSession.builder().appName(this.getClass.getSimpleName).
     master("local[*]").getOrCreate()
    import spark.implicits._
    val tp: Dataset[(String, String)] = spark.createDataset(List(("aaa","bbb"),("aaa","ccc"),("aaa","ddd")))
    val df: DataFrame = tp.toDF("f1","f2")
    //df.selectExpr("concat_ws('-',f1,f2) as f3").show()
    import org.apache.spark.sql.functions._
    df.select(concat_ws("-",$"f1",$"f2") as "f3").show()
    //MY_CONCAT_WS 函数名称
    //后面传入的scala的函数就是具有的实现逻辑
    df.createTempView("v_data")
    spark.udf.register("MY_CONCAT_WS",(s:String,first:String,second:String) => {
      s + first + second
    })
    df.selectExpr("MY_CONCAT_WS('|',f1,f2) as f3").show()
    spark.stop()
  }
}
