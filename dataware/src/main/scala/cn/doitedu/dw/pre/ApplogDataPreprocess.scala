package cn.doitedu.dw.pre

import cn.doit.edu.commons.util.SparkUtil
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.Dataset

/**
  * App埋点日志预处理
  *
  * @author charley
  * @create 2020-12-22-22
  */
object ApplogDataPreprocess {
  def main(args: Array[String]): Unit = {
    val session = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import session.implicits._
    //加载当日app埋点日志数据
    val ds: Dataset[String] = session.read.textFile("D:\\doit_yiee_logs\\2020-12-21\\app")
    //解析json
    ds.map(line=>{
      val jsonobj = JSON.parseObject(line)
      //解析，抽取各个字段

      //返回结果:如果数据符合要求，则返回一个AppLogBean，如果不符合要求，就返回一个null
    null
    })
      //过滤掉不符合要求的数据
      .filter(_!=null).map(bean=>{
      //数据集成 省市区

      //数据集成guid

    }).toDF().write.parquet()

  }
}
