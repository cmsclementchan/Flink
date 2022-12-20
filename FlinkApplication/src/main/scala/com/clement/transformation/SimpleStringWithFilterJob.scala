package com.clement.transformation

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

import java.util.Properties

object SimpleStringWithFilterJob {
  def main(args: Array[String]): Unit = {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "com.clement")

    // To create an object for jsonSchema
    val jsonSchema = new JSONKeyValueDeserializationSchema(false)
    // To create the consumer
    val consumer = new FlinkKafkaConsumer("quickstart-events", jsonSchema, properties)


    // check if the consumer get any data
    // val stream = env.addSource(consumer).print()

    // check if the consumer can read the json properly
    val stream = env.addSource(consumer)
    val result = stream
      .filter(x => x != "Clement")
      .print()

    env.execute("Flink to Filter in kafka topic")

  }


}
