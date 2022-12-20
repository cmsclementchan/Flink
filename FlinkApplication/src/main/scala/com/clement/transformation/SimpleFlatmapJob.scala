package com.clement.transformation

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties


object SimpleFlatmapJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    //properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "com.clement")

    val stream = env
      .addSource(new FlinkKafkaConsumer[String]("quickstart-events", new SimpleStringSchema(), properties))
      .flatMap( str => str.split(" "))
      .print()

    env.execute("Flink Streaming Scala API Skeleton")


  }
}
