package cn.doitedu.dw.sql

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.DayOfMonth
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * @author charley
  * @create 2021-01-19-19
  * 使用SparkCore计算连续登陆三天及以上的用户
  */
object UserContinuedLoginSQL1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("dataware/access.csv")
    //将数据处理，根据uid进行分区，在分区内按照日期排序，按照顺序打行号
    val uidDateAndNull: RDD[((String, String), Null)] = lines.map(line=>{
      val fields = line.split(",")
      val uid = fields(0)
      val date = fields(1)
      ((uid,date),null)
    })
    //先计算出用户id的数量
    val uids: Array[String] = uidDateAndNull.keys.map(_._1).distinct().collect()
    //按照用户id进行分区，然后在分区内排序
    val sortedInpartition: RDD[((String, String), Null)] = uidDateAndNull.repartitionAndSortWithinPartitions(new Partitioner {
      val idToPartitionId = new mutable.HashMap[String,Int]
      for(i<- uids.indices){
        idToPartitionId(uids(i)) = i
      }
      override def numPartitions: Int = uids.length
      override def getPartition(key: Any): Int = {
        val tp = key.asInstanceOf[(String,String)]
        idToPartitionId(tp._1)
      }
    })
    val uidTimeAndDate = sortedInpartition.mapPartitions(it=>{
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val calendar = Calendar.getInstance()
      var i = 0
      it.map(t=>{
        i += 1
        val uid = t._1._1
        val dateStr = t._1._2
        val date = sdf.parse(dateStr)
        calendar.setTime(date)
        calendar.add(Calendar.DATE,-i)
        val time = calendar.getTime.getTime
        ((uid,time),dateStr)
      })
    })
    //按照uid,time分组，求count("*"),min("dt"),max("dt") from ... group by uid,time having counts>=3
    val result: RDD[(String, String, String, Int)] = uidTimeAndDate.groupByKey().mapValues(it=>{
      val sorted = it.toList.sorted
      //连续登陆的次数
      val counts = sorted.size
      //起始时间
      val start_date = sorted.head
      //截至时间
      val end_date = sorted.last
      (start_date,end_date,counts)
    }).filter(_._2._3 >=3).map(t=>{
      (t._1._1,t._2._1,t._2._2,t._2._3)
    })
    println(result.collect().toBuffer)
    sc.stop()
  }
}
