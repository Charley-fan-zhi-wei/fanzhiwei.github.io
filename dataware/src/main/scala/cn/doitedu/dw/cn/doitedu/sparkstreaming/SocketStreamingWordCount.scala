package cn.doitedu.dw.cn.doitedu.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author charley
  * @create 2021-01-21-21
  * 从指定的socket端口中实时读取数据，实时WordCount统计
  */
object SocketStreamingWordCount {
  def main(args: Array[String]): Unit = {
    //使用SparkContext创建RDD
    //StreamContext,是对SparkContext包装和增强,可以用S
    //SparkStreaming也有一个抽象的数据集DStream【离散的数据流】,是对RDD的封装
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //StreamingContext是对SparkContext的包装和增强
    //StreamingContext是对SparkContext包装和增强，后面传入的时间代表一个批次产生的时间
    val stc = new StreamingContext(sc,Seconds(5))
    //使用StreamingContext创建DStream
    val lines: DStream[String] = stc.socketTextStream("192.168.1.109",8888)
    //对DStream进行操作
    val reduced: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    reduced.foreachRDD(rdd=>{

    })
    //Action
    reduced.print()
    //开启spark streaming
    stc.start()
    //让程序挂起，一直运行
    stc.awaitTermination()

  }
}
