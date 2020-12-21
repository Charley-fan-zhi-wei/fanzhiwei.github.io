import org.apache.spark.sql.SparkSession

/**
  * @author charley
  * @create 2020-12-20-20
  */
object ReadParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local").getOrCreate()
    val df = spark.read.parquet("data/dict/geo_dict/output")
    df.show(1,false)
    spark.close()
  }
}
