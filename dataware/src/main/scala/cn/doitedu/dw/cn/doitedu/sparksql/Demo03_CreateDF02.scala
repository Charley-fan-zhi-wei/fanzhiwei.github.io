package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
/**
  * @author charley
  * @create 2021-01-17-17
  */
object Demo03_CreateDF02 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val rdd: RDD[String] = spark.sparkContext.textFile("dataware/stu.txt")
    //1.将RDD[String]编程RDD[(f1,f2,f3)]
    val rddTuple: RDD[(Int, String, Int, String, Double)] = rdd.map(_.split(",")).map(arr=>(arr(0).toInt,arr(1),arr(2).toInt,arr(3),arr(4).toDouble))
    val df = rddTuple.toDF("id","name","age","city","score")
    df.printSchema()
    df.show()
  }
}
