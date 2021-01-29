package cn.doitedu.dw.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author charley
  * @create 2021-01-19-19
  */
object IpToLocationSQL {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[*]").getOrCreate()
    import spark.implicits._
    val ds: Dataset[String] = spark.createDataset(List("125.213.100.123","117.101.215.33"))
    val ips: DataFrame = ds.toDF("ip")
    //自定义一个函数，根据ip地址计算出ip对应的为止【UDF】
    //125.213.100.123 -> 辽宁省,本溪市
    //注册一个函数
    spark.udf.register("it2Loc",(ip:String)=>{
      //将ip地址转成10进制
      val ipNum = IpUtils.ip2Long(ip)
      //二分法查找

    })
    ips.selectExpr("it2Loc(ip)").show()
    spark.close()
  }
}
