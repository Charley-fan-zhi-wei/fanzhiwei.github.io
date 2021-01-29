package cn.doitedu.dw.idmp

import cn.doit.edu.commons.util.SparkUtil
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

/**
  * 考虑上一日的id映射字典整合的idmapping程序
  * @author charley
  * @create 2020-12-21-21
  */
object LogDataIdmoV2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    //一.加载各类日数据
    val appLog: Dataset[String] = spark.read.textFile("D:\\doit_yiee_logs\\2020-12-22\\app")
    val webLog = spark.read.textFile("D:\\doit_yiee_logs\\2020-12-22\\web")
    val wxAppLog = spark.read.textFile("D:\\doit_yiee_logs\\2020-12-22\\wx_app")
    //二.提取每一类数据中每一行的标识id
    val appIds: RDD[Array[String]] = extractIds(appLog)
    val webIds: RDD[Array[String]] = extractIds(webLog)
    val wxIdx: RDD[Array[String]] = extractIds(wxAppLog)
    val ids: RDD[Array[String]] = appIds.union(webIds).union(wxIdx)
    //三.构造图计算中的vertex集合
    val vertices: RDD[(Long, String)] = ids.flatMap(arr=>{
      for(biaoshi <- arr) yield(biaoshi.hashCode.toLong,biaoshi)
    })
    //四.构造图计算的边集合
    //{a,b,c} a b a c b c
    val edges: RDD[Edge[String]] = ids.flatMap(arr => {
      //用双重for循环，来对一个数组中所有的标识进行两两组合成边
      for (i <- 0 to arr.length - 2; j <- i + 1 to arr.length - 1)
        yield Edge(arr(i).hashCode.toLong, arr(j).hashCode.toLong, "")
    })
      //将边变成(边,1)
      .map(edge => (edge, 1)).reduceByKey(_ + _)
      //过滤掉出现次数小于经验阈值的边
      .filter(tp => tp._2 >=1).map(tp=>tp._1)

    //五.将上一日的idmp映射字典，解析成点，边集合
    val preDayIdmp = spark.read.parquet("data/idmp/2020-12-21")
    //构造点集合
    val preDayIdmpVertices = preDayIdmp.rdd.map({
      case Row(idFlag:VertexId, guid:VertexId) =>
        (idFlag, "")
    })
    //构造边集合
    val preDayEdges = preDayIdmp.rdd.map(row => {
      val idFlag = row.getAs[VertexId]("biaoshi_hashcode")
      val guid = row.getAs[VertexId]("guid")
      Edge(idFlag, guid, "")
    })
    //六.将当日的点集合union上日的点集合，当日的边集合union上日的边集合
    //七.用点集合+边集合构造图,并调用最大连通子图算法
    val graph = Graph(vertices.union(preDayIdmpVertices),edges.union(preDayEdges))
    //VertexRDD[vertexId] -> RDD[(点id long,组中的最小值)]
    val res_tuples: VertexRDD[VertexId] = graph.connectedComponents().vertices
    //八.将结果跟上日的映射字典做对比，映射guid
    //1.将上日的idmp映射结果字典收集到driver端,并广播
    //2.将今日的图计算结果按照guid分组
    val idMap = preDayIdmp.rdd.map(row => {
      val idFlag = row.getAs[VertexId]("biaoshi_hashcode")
      val guid = row.getAs[VertexId]("guid")
      (idFlag, guid)
    }).collectAsMap()
    val bc = spark.sparkContext.broadcast(idMap)
   val todayIdmpResult = res_tuples.map(tp=>(tp._2,tp._1)).groupByKey().mapPartitions(iter=>{
      //从广播变量中取出上日的idmp映射字典
      val idmpMap = bc.value
      iter.map(tp=>{
        //当日的guid计算结果
        var todayGuid = tp._1
        //这一组中的所有id标识
        val ids = tp._2
        //遍历这一组id,挨个去上日的idmp映射字典中查找
        var find = false
        for(element<-ids if !find){
          val maybeGuid: Option[VertexId] = idmpMap.get(element)
          //如果这个id在昨天的映射字典中找到了，那么就用昨天的guid替换掉今天这一组的guid
          if(maybeGuid.isDefined) {
            todayGuid = maybeGuid.get
            find = true
          }
        }
        (todayGuid,ids)
      })
    }).flatMap(tp=>{
     val ids = tp._2
     val guid = tp._1
     for(element<-ids) yield(element,guid)
   })

    //九.可以直接使用图计算所产生的结果中的组最小值，作为这一组的guid(当然，也可以自己另外生成一个uuid来作为guid)
    import spark.implicits._
    //保存结果
    todayIdmpResult.coalesce(1).toDF("biaoshi_hashcode","guid").write.parquet("data/idmp/2020-12-22")
    spark.close()
  }
  /*
  * 从一个日志ds中提取各类标识id
  * */
  def extractIds(logDs:Dataset[String]):RDD[Array[String]] = logDs.rdd.map(line=>{
    //将一行数据解析成json对象
    val jsonObj = JSON.parseObject(line)
    //从json对象中取user对象
    val userObj = jsonObj.getJSONObject("user")
    //从user对象中取uid
    val uid = userObj.getString("uid")
    val phoneObj = userObj.getJSONObject("phone")
    val imei = phoneObj.getString("imei")
    val imsi = phoneObj.getString("imsi")
    Array(uid,imei,imsi).filter(StringUtils.isNotBlank(_))
  })
}
