package com.clement.transformation

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

import java.util.Properties


object SimpleReduceJob {

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

    // check if the consumer can read the json properly
    val stream: DataStream[(String, Int, Int)] = env
      .addSource(consumer)
      .map(obj => {
        val name = obj.get("value").get("name") // get the key = "name" 's value
        val age = obj.get("value").get("age") // get the key = "age" 's value
        val volume = obj.get("value").get("volume") // get the key = "age" 's value
        (name.asText, age.asInt, volume.asInt)

      })
      .keyBy(x => x._1)
      .reduce((s1, s2) => (s1._1, s1._2, s1._3 + s2._3))


      val result = stream.print()

    env.execute("Reduce transformation")
    // https://github.com/streaming-with-flink/examples-scala/blob/master/src/main/scala/io/github/streamingwithflink/chapter6/WindowFunctions.scala

  }
}
