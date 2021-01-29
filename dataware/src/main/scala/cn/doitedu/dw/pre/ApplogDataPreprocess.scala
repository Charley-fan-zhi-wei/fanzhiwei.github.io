package cn.doitedu.dw.pre

import java.util

import ch.hsr.geohash.GeoHash
import cn.doit.edu.commons.util.SparkUtil
import cn.doitedu.dw.beans.AppLogBean
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * App埋点日志预处理
  *
  * @author charley
  * @create 2020-12-22-22
  */
object ApplogDataPreprocess {
  def main(args: Array[String]): Unit = {

    /*val session = SparkUtil.getSparkSession(this.getClass.getSimpleName)*/
    val session = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import session.implicits._
    //加载当日app埋点日志数据

    //val ds: Dataset[String] = session.read.textFile("D:\\doit_yiee_logs\\2020-12-21\\app")
    val ds: Dataset[String] = session.read.textFile(args(0))

    //加载省市区字典，并收集到Driver端，然后广播出去
   /* val geoDf = session.read.parquet("data/dict/geo_dict/output").rdd.map({ case Row(geo:String
    ,province:String,city:String,district:String) =>(geo,(province,city,district))
    }).collectAsMap()*/
   val geoDf = session.read.parquet(args(1)).rdd.map({ case Row(geo:String
   ,province:String,city:String,district:String) =>(geo,(province,city,district))
   }).collectAsMap()


    val bc_geo = session.sparkContext.broadcast(geoDf)

    //加载idmp映射字典，并收集到Driver端，然后广播出去
   /* val idmp_map = session.read.parquet("data/idmp/2020-12-22").rdd.map({ case Row(biaoshi_hascode: Long, guid: Long) =>
      (biaoshi_hascode, guid)
    }).collectAsMap()*/

    val idmp_map = session.read.parquet(args(2)).rdd.map({ case Row(biaoshi_hascode: Long, guid: Long) =>
      (biaoshi_hascode, guid)
    }).collectAsMap()


    val bc_idmp = session.sparkContext.broadcast(idmp_map)
    //解析json
   var res =  ds.map(line=>{
      try {
        val jsonobj: JSONObject = JSON.parseObject(line)
        //解析，抽取各个字段
        val eventid: String = jsonobj.getString("eventid")
        import scala.collection.JavaConversions._
        val eventMap: Map[String, String] = jsonobj.getJSONObject("event").
            getInnerMap.asInstanceOf[util.Map[String,String]].toMap
        val userObj = jsonobj.getJSONObject("user")
        val uid = userObj.getString("uid")

        val phone = userObj.getJSONObject("phone")
        val imei = phone.getString("imei")
        val imsi = phone.getString("imsi")

        val locObj = jsonobj.getJSONObject("loc")
        val longtitude = locObj.getDouble("longtitude")
        val latitude = locObj.getDouble("latitude")

        val timestamp = jsonobj.getString("timestamp").toLong

        //判断数据是否符合规则
        //1.标识字段不能全为空
        val sb = new StringBuilder
        val flagFields = sb.append(uid).append(imei).append(imsi).toString().replaceAll("null","")
        var bean:AppLogBean = null;
        if(StringUtils.isNotBlank(flagFields)&&eventMap!=null
          &&StringUtils.isNotBlank(eventid)){
          //返回一个正常的对象
          bean = AppLogBean(
            Long.MinValue,
            eventid,
            eventMap,
            uid,
            imei,
            imsi,
            longtitude,
            latitude,
            timestamp
          )
        }
        bean
      } catch {
        case e:Exception  => null
      }
    })
      //过滤掉不符合要求的数据
      .filter(_!=null).map(bean=>{

        //取出广播变量中的字典
      val geoDict: collection.Map[String, (String, String, String)] = bc_geo.value
      val idmpDict: collection.Map[Long, Long] = bc_idmp.value
      //数据集成 省市区
      val lng = bean.longtitude
      val lat = bean.latitude
      val geo = GeoHash.geoHashStringWithCharacterPrecision(lat,lng,5)
      val maybeTuple: Option[(String, String, String)] = geoDict.get(geo)
      if(maybeTuple.isDefined){
        val area: (String, String, String) = maybeTuple.get
        bean.province = area._1
        bean.city = area._2
        bean.district = area._3
      }
     //数据集成guid
     var ids = Array(bean.imei,bean.imsi,bean.uid)
     var find = false
     for (elem <- ids if!find) {
       val maybeLong: Option[Long] = idmpDict.get(elem.hashCode.toLong)
       if(maybeLong.isDefined){
        bean.guid = maybeLong.get
         find = true
       }
     }
     bean
    }).filter(bean=>bean.guid!=Long.MinValue).toDF().write.parquet(args(3))
    //parquet("data/applog_processed/2020-12-21")
    session.close()
  }
}
