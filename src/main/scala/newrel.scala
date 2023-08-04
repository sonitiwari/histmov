package itc.org.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object newrel extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "kaf")

   val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  import spark.implicits

   val df = spark.read.option("header",true).option("inferschema", true).csv("/user/ec2-user/junebatch/sonika/movie2022.csv")


  df.show(5)

}
