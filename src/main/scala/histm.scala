package itc.org.com

import org.apache.log4j.{Level,Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode,SparkSession}

object histm extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "kaf")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits

  val df = spark.read.option("header", true).option("inferschema", true).csv("C:\\Users\\A\\Desktop\\tmr\\historical data")
   df.filter("vote_average==8").show(5)
   df.filter("adult==False").show(5)
   df.filter("vote_count>30000").show(5)




}
