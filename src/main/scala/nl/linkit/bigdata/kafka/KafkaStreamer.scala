package nl.linkit.bigdata.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object KafkaStreamer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._
    val rStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "in2")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val wStream = rStream
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "out2")
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("append")
      .option("checkpointLocation", "/var/tmp")
      .start()

    wStream.awaitTermination()
  }
}
