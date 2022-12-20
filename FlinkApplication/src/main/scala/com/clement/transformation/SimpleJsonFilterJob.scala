package com.clement.transformation

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

import java.util.Properties

object SimpleJsonFilterJob {
  def main(args: Array[String]): Unit = {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "com.clement")


    // To create an object for jsonSchema
    val jsonSchema = new JSONKeyValueDeserializationSchema(false)
    // To create the consumer
    val consumer = new FlinkKafkaConsumer("quickstart-events",jsonSchema, properties)


    // check if the consumer get any data
    // val stream = env.addSource(consumer).print()

    // check if the consumer can read the json properly
    val stream = env.addSource(consumer)
    val result = stream
      .map(obj => {
          val name = obj.get("value").get("name") // get the key = "name" 's value
          val age = obj.get("value").get("age")// get the key = "name" 's value
          (name,age.asInt)
        })
      .filter( obj => obj._2 > 20 ) // the tuple from the map , and get the second field.
      .print()

    env.execute("Flink to get JSON object in kafka topic")

  }

  // Exercise
  // So, how to change the above to use the following Function
  class MyFilterFunction(limit: Int) extends RichFilterFunction[Int] {

    override def filter(input: Int): Boolean = {
      if (input > limit) {
        true
      } else {
        false
      }
    }

  }


}


