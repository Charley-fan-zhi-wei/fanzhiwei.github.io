package cn.doitedu.dw.cn.doitedu.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author charley
  * @create 2021-01-21-21
  */
object UpdateStateWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    //设置日志级别
    sc.setLogLevel("WARN")
    //创建StreamingContext并指定批次生成的时间
    val stc = new StreamingContext(sc,Seconds(5))
    //设置checkpoint目录，保存历史状态
    stc.checkpoint("./ck")
    //创建DStream
    val lines: ReceiverInputDStream[String] = stc.socketTextStream("192.168.1.109",8899)
    //对数据进行处理，累加历史批次数据
    //累加历史数据就要将中间结果保存起来【要求保存到靠谱的存储系统(安全),以后任务失败可以恢复State数据】
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    //(Seq[V], Option[S]) => Option[S]
    //第一个参数，当前批次计算的结果
    //第二个参数，初始值或中间累加结果
    val updateFunc = (s:Seq[Int],o:Option[Int]) =>{
      //将当前批次累加的结果和初始值或中间结果进行累加
      Some(s.sum + o.getOrElse(0))
    }
    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc)
    //触发Action
    reduced.print()
    stc.start()
    //挂起一直运行
    stc.awaitTermination()
  }
}
