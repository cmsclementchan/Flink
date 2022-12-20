package com.clement.transformation

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

import java.util.Properties

object SimpleTumblingWindowJob {

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
    val stream = env
      .addSource(consumer)
      .map(obj => {
        val name = obj.get("value").get("name") // get the key = "name" 's value
        val age = obj.get("value").get("age") // get the key = "age" 's value
        val volume = obj.get("value").get("volume") // get the key = "age" 's value

        (name.asText, age.asInt, volume.asInt)
      })
      .keyBy(new MyKeySelector)
      //      .keyBy(x => x._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .sum(2) // apart from the key, the rest 's position
      .print()


    // keyBy, it will remember the last value and sum
    // Example DATA : {"name":"clement","age":"18","volume":2}
    // Example DATA : {"name":"jack","age":"18","volume":5}
    // Example DATA : {"name":"clement","age":"18","volume":2}
    env.execute("basic keyBy transformation")

  }


  class MyKeySelector extends KeySelector[(String, Int, Int), String] {
    override def getKey(in: (String, Int, Int)): String = {
      return in._1
    }
  }

}