package com.guavus.vzb.streaming

import com.databricks.spark.avro.SchemaConverters
import com.twitter.bijection.Injection
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.from_unixtime
import org.apache.log4j.Logger
import com.guavus.vzb.util.Util
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.functions.udf

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

    val parser = new Schema.Parser
    val schema = parser.parse(source)
    val recordInjection: Injection[GenericRecord,Array[Byte]] = GenericAvroCodecs.toBinary(schema)
    val sqlType = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]

    val data = spark
      .readStream
      .format("kafka")
      .options(kafkaProps)
      .load()

      def deserialize: (Array[Byte]) => Row = (data: Array[Byte]) => {
        val parser = new Schema.Parser
        val schema = parser.parse(source)
        val recordInjection: Injection[GenericRecord,Array[Byte]] = GenericAvroCodecs.toBinary(schema)
        val record = recordInjection.invert(data).get
        val objectArray = new Array[Any](record.asInstanceOf[GenericRecord].getSchema.getFields.size)
        record.getSchema.getFields.asScala.foreach(field => {
          val fieldVal = record.get(field.pos()) match {
            case x:org.apache.avro.util.Utf8 => x.toString
            case y:Any => y
          }
          objectArray(field.pos()) = fieldVal
        })
        Row(objectArray:_*)
      }

    val udf_deserialize = udf(deserialize, DataTypes.createStructType(sqlType.fields))

    val ds2 = data.select("value").as(Encoders.BINARY)
        .withColumn("rows", udf_deserialize($"value"))
        .select("rows.*")

    //      .select("value.*")
    //      .withColumn("year", from_unixtime($"sn_flow_end_time","yyyy"))
    //      .withColumn("month",from_unixtime($"sn_flow_end_time","MM"))
    //      .withColumn("day",from_unixtime($"sn_flow_end_time","dd"))
    //      .withColumn("hour",from_unixtime($"sn_flow_end_time","HH"))

    ds2.printSchema()

    val query = ds2
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
