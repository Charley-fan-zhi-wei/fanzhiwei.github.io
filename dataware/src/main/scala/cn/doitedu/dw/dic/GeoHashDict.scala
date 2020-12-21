package cn.doitedu.dw.dic

import java.util.Properties

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SparkSession

/**
  * @author charley
  * @create 2020-12-20-20
  */
object GeoHashDict {
  def main(args: Array[String]): Unit = {
    //构造spark
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[*]").getOrCreate()
    import spark.implicits._
    //读取mysql中的gps坐标地理位置表
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    val df = spark.read.jdbc("jdbc:mysql://localhost:3306/dicts","tmp",props)
    //将gps坐标通过geohash算法转成geohash编码
    val res = df.map(row=>{
      //取出这一行的经纬度
      val lng = row.getAs[Double]("BD09_LNG")
      val lat = row.getAs[Double]("BD09_LAT")
      val province = row.getAs[String]("province")
      val city = row.getAs[String]("city")
      val district = row.getAs[String]("district")
      //调用geohash算法，得出geohash编码
      val geoCode = GeoHash.geoHashStringWithCharacterPrecision(lat,lng,5)
      //组装返回结果
      (geoCode,province,city,district)
    }
    ).toDF("geo","province","city","district")
    //保存结果
    //res.show(10,false)
    res.write.parquet("data/dict/geo_dict/output")
    spark.close()
  }
}
