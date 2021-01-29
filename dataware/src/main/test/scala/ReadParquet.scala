import org.apache.commons.codec.digest.DigestUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.bouncycastle.jcajce.provider.digest.SHA224.Digest


/**
  * @author charley
  * @create 2020-12-20-20
  */
object ReadParquet {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local").getOrCreate()
    val df = spark.read.parquet("data/idmp/2020-12-22")
    df.show(20,false)
    val str1 = DigestUtils.md5Hex("1231jk23k1k3")
    println(str1)
    spark.close()
  }
}
