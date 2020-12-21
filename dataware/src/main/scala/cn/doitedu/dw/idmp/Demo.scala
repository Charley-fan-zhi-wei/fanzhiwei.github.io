package cn.doitedu.dw.idmp

import cn.doit.edu.commons.util.SparkUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.sql.Dataset

import scala.collection.mutable.ListBuffer

/** 为了做idmapping来写的一个图计算使用demo，找出哪些标识是同一个人
  * 13866778899,刘德华,wx_hz,2000
  * 13877669988,华仔,wx_hz,3000
  * ,刘德华,wx_1dh,5000
  * 13912344321,马德华,wx_mdh,12000
  * 13912344321,二师兄,wx_bj,3500
  * 13912664321,猪八戒,wx_bj,5600
  *
  * @author charley
  * @create 2020-12-20-20
  */
object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._
    //加载原始数据
    val ds: Dataset[String] = spark.read.textFile("data/graphx/input")
    //构造一个点RDD
    var vertices = ds.rdd.flatMap(line=>{
      val fields = line.split(",")
      for(element<-fields if StringUtils.isNotBlank(element)) yield(element.hashCode.toLong,element)


      //在spark图计算api中，点需要表示成一个tuple-> (点的唯一标识Long,点的数据)
    /*  Array((fields(0).hashCode.toLong,fields(0)),(fields(1).hashCode.toLong,fields(1))
        ,(fields(2).hashCode.toLong,fields(2))
      )*/
    })
    //构造一个边RDD spark graphx中对边的概述结果: Edge(起始点id,目标点id,边数据)
    val edges =
    ds.rdd.flatMap(line=>{
      val fields = line.split(",")
      /*val lst = new ListBuffer[Edge[String]]()
      for(i<- 0 to fields.length-2){
        val edge1 = Edge(fields(i).hashCode.toLong,fields(i+1).hashCode.toLong,"")
        lst += edge1
      }
      lst*/
      for(i<- 0 to fields.length-2 if StringUtils.isNotBlank(fields(i)))
        yield Edge(fields(i).hashCode.toLong,fields(i+1).hashCode.toLong,"")
    })
    //用点集合和边集合构造一张图
    val graph = Graph(vertices,edges)
    //调用图的算法
    val graph2 = graph.connectedComponents()
    val vertices2: VertexRDD[VertexId] = graph2.vertices
    //(点id-0,点数据-0)
    //(点id-1,点数据-0)
    //(点id-4,点数据-4)
    //(点id-5,点数据-4)
    //vertices2.take(30).foreach(println)
    /*
    * (-1095633001,-1095633001)
      (29003441,-1095633001)
      (-774395369,-1485777898)
      (113568560,-1485777898)
      (1567005,-1485777898)
      (113568358,-1095633001)
      (-1485777898,-1485777898)
      (0,-1485777898)
      (-1007898506,-1095633001)
      (681286,-1485777898)
      (-774337709,-1095633001)
      (20977295,-1485777898)
      (1571810,-1095633001)
      (46789743,-1095633001)
      (1632353,-1095633001)
      (208397334,-1485777898)
      (20090824,-1095633001)
      (1626587,-1485777898)
      (1537214,-1485777898)
      (38771171,-1095633001)
    * */
    //利用映射关系结果，来加工原始数据
    //将上面得到的映射关系rdd，收集到Driver端，然后作为变量广播出去
    val idmpMap = vertices2.collectAsMap()
    val bc = spark.sparkContext.broadcast(idmpMap)

    val res = ds.map(line=>{
      val bc_map = bc.value
      val name = line.split(",").filter(StringUtils.isNotBlank(_))(0)
      val gid = bc_map.get(name.hashCode.toLong).get

      gid+","+line
    })
    res.show(10,false)
    spark.close()
  }
}
