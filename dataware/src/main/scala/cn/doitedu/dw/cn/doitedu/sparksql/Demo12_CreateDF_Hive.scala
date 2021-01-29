package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author charley
  * @create 2021-01-18-18
  */
object Demo12_CreateDF_Hive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).
      master("local[*]").enableHiveSupport().getOrCreate()
  /*  val r1 = (1,"zs",28)
    val r2 = (2,"ls",38)
    val rdd = spark.sparkContext.parallelize(List(r1,r2))
    import spark.implicits._
    val df2 = rdd.toDF("id","name","age")
    df2.createTempView("t3")*/
    val df: DataFrame = spark.table("t3")
    df.show()
    df.where("array_contains(map_keys(jiaren),'b')").show()
    spark.close()
  }
}
