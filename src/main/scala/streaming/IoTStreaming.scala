package com.guavus.vzb.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.from_unixtime
import org.apache.log4j.Logger

import com.guavus.vzb.util.Util
import scala.io.Source

object IoTStreaming {

  @transient lazy val log = Logger.getLogger(getClass.toString)

  def main(args: Array[String]): Unit = {

    val iotConfigFile = args(0)
    val sparkConfigFile = args(1)
    val kafkaConfigFile = args(2)

    import scala.collection.JavaConverters._
    val iotProps = Util.loadProperties(iotConfigFile).asScala
    val sparkProps = Util.loadProperties(sparkConfigFile).asScala
    val kafkaProps = Util.loadProperties(kafkaConfigFile).asScala

    val master = iotProps.getOrElse("master","local[*]")

    val spark = SparkSession.builder.master(master).appName("IoTStreaming").getOrCreate()
    import spark.implicits._

    val avroSchemaFile = iotProps.getOrElse("schema_file", throw new RuntimeException("Can't find schema file..."))
    val outputPath = iotProps.getOrElse("output_path", throw new RuntimeException("Can't find output directory..."))
    val checkpointLocation = iotProps.getOrElse("checkpoint_location", throw new RuntimeException("Can't find check point directory..."))

    sparkProps.foreach(prop => spark.conf.set(prop._1, prop._2))

    val source = Source.fromInputStream(Util.getInputStream(avroSchemaFile)).getLines.mkString

    val data = spark
      .readStream
      .format("kafka")
      .options(kafkaProps)
      .option("avroSchema",source)
      .load()
      .select($"value")
    //      .select("value.*")
    //      .withColumn("year", from_unixtime($"sn_flow_end_time","yyyy"))
    //      .withColumn("month",from_unixtime($"sn_flow_end_time","MM"))
    //      .withColumn("day",from_unixtime($"sn_flow_end_time","dd"))
    //      .withColumn("hour",from_unixtime($"sn_flow_end_time","HH"))

    data.printSchema()

    val query = data
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("append")
      .format("parquet")
      .option("path",outputPath)
      .option("checkpointLocation",checkpointLocation)
      .start()

    log.info("Update partition here")

    query.awaitTermination()
  }

}
