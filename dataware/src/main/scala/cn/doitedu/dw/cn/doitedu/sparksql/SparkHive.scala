package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author charley
  * @create 2021-01-19-19
  */
object SparkHive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val spark: SparkSession = SparkSession.builder().appName("SparkHive").master("local[*]").enableHiveSupport().getOrCreate()
    //读取hdfs中的非结构化数据，对数据进行处理
    //在hive中建表
    spark.sql("create table person(id bigint,name string,age int) row format delimited fields " +
      "terminated by ','")
    spark.sql("load data local inpath 'dataware/person.txt'into table person")
    val df: DataFrame = spark.sql("select * from person where id > 2")
    df.show()
    spark.close()
  }
}
