package cn.doitedu.dw.cn.doitedu.sparksql

import java.lang

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * @author charley
  * @create 2021-01-20-20
  */
object GeoMeanUDAFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      .getOrCreate()
    val ids: Dataset[lang.Long] = spark.range(1,11)
    val geo_mean = new GeoMean
    spark.udf.register("geo_mean",geo_mean)
    import spark.implicits._
    ids.agg(geo_mean($"id").as ("mean")).show()
    spark.close()
  }
}
class GeoMean extends  UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    StructType(List(
      StructField("num",LongType)
    ))
  }

  override def bufferSchema: StructType = {
    StructType(List(
      StructField("mean",LongType),
      StructField("nums",IntegerType)
    ))
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 1L
    buffer(1) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) * input.getLong(0)
    buffer(1) = buffer.getInt(1) + 1
  }
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) * buffer2.getLong(0) //全局的乘积
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)   //全局的数量
  }

  override def evaluate(buffer: Row): Any = {
    val i = buffer.getLong(0)
    val j = buffer.getInt(1)
    Math.pow(i,1.toDouble/j)
  }
}