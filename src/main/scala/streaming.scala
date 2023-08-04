import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object streaming extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("My Streaming Application")
    .config("spark.sql.shuffle.partitions", 3)
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .config("spark.sql.streaming.schemaInference","true").getOrCreate()

  //1.Read from file source
  val ordersDf = spark.readStream
    .format("csv")
    .option("path","input").load()


  // 2. process
  ordersDf.createOrReplaceTempView("movie")
  val completedOrders = spark.sql("select * from movie ")


  //3.Write to the sink
  val ordersQuery = completedOrders.writeStream
    .format("json")
    .outputMode("append")
    .option("path","output")
    .option("checkpointLocation","checkpoint-location7")
    .trigger(Trigger.ProcessingTime("7 days"))
    .start()

  ordersQuery.awaitTermination()
}