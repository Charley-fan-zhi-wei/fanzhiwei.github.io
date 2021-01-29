package cn.doitedu.dw.cn.doitedu.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.internal.util.TableDef.Column

/**
  * @author charley
  * @create 2021-01-18-18
  */
object Demo14_DML_DSLapi {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession()
    val df: DataFrame = spark.read.option("header","true").csv("dataware/stu2.csv")
      //rowOp(spark,df)
    //  whereOp(spark,df)
    groupBy(spark,df)
    spark.close()
    def rowOp(spark:SparkSession,df:DataFrame)={
      import spark.implicits._
      //df.select("id","name","age","city").show()
      //df.select($"id",$"name")
      import org.apache.spark.sql.functions._
      //df.select(col("id"),col("name"),col("age")+10).show()
      //df.select('id,'name)
      df.selectExpr("age+10 as age2").show()
    }

    def whereOp(spark:SparkSession,df:DataFrame)={
      df.where("id>3").show()
      df.where("id>3 and score > 85")
    }

    def groupBy(spark:SparkSession,df:DataFrame)={
      import org.apache.spark.sql.functions._
      df.selectExpr("cast(id as int) as id","name","cast(age as int) as age","sex","city","cast(score as double) as score").
        groupBy("city").avg("score").select("city","avg(score)").show()
      df.selectExpr("cast(id as int) as id","name","cast(age as int) as age","sex","city","cast(score as double) as score").
        groupBy("city").agg("score"->"sum","score"->"avg").show()
    }

    /**
      *
      * @param spark
      * @param df
      */
    def joinOp(spark:SparkSession,df:DataFrame)={




    }
  }
}
