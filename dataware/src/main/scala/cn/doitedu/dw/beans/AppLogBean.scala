package cn.doitedu.dw.beans

/**
  * @author charley
  * @create 2020-12-22-22
  *         封装app埋点日志的case class
  */
case class AppLogBean(
                       var guid: Long,
                       eventid: String,
                       event: Map[String, String],
                       uid: String,
                       imei: String,
                       imsi: String,
                       longtitude: Double,
                       latitude: Double,
                       timestamp: Long,
                       var province: String = "未知",
                       var city: String = "未知",
                       var district: String = "未知"
                     )
