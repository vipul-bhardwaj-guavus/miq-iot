package com.guavus.vzb.streaming

import java.util.Properties

import com.guavus.vzb.util.Util
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.io.Source

object KafkaProducerTest {

  def getKafkaProducer = {
    val properties = new Properties
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("acks", "all")
    properties.put("retries", 0.asInstanceOf[java.lang.Integer])
    properties.put("batch.size", 16384.asInstanceOf[java.lang.Integer])
    properties.put("linger.ms", 1.asInstanceOf[java.lang.Integer])
    properties.put("buffer.memory", 33554432.asInstanceOf[java.lang.Integer])
    properties.put("key.serializer", classOf[StringSerializer])
    properties.put("value.serializer", classOf[ByteArraySerializer])

    new KafkaProducer[String, Array[Byte]](properties)
  }

  def generate_test_data = {
    val avroSchemaFile = "/Users/vipulbhardwaj/IdeaProjects/miq-iot/src/main/resources/test.avsc"
    val parser: Parser = new Schema.Parser
    val source = Source.fromInputStream(Util.getInputStream(avroSchemaFile)).getLines.mkString
    println(source)
    val schema = parser.parse(source)

    val topic_name = "iot-test3"

    val producer = getKafkaProducer

    for(i <- 1 to 10) {
      val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
      val record = new GenericData.Record(schema)
      record.put("server_ip","localhost")
      val bytes = recordInjection.apply(record)
      producer.send(new ProducerRecord[String,Array[Byte]](topic_name, bytes))
      println(s"$i Message sent successfully ${record.toString}")
    }
    producer.close()
  }

  def main(args: Array[String]): Unit = {
    generate_test_data
  }

}
