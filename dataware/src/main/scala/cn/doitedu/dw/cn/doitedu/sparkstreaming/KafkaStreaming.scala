package cn.doitedu.dw.cn.doitedu.sparkstreaming

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author charley
  * @create 2021-01-21-21
  */
object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //StreamingContext是对SparkContext的包装和增强
    //StreamingContext是对SparkContext包装和增强，后面传入的时间代表一个批次产生的时间
    val stc = new StreamingContext(sc,Seconds(5))
    //跟kafka整合,创建直连的DStream【使用底层的消费API,效率更高】
    //KafkaUtils.createDirectStream(stc,
     // LocationStrategies.PreferConsistent, //位置策略
      //ConsumerStrategies.Subscribe()
    )
  }
}
