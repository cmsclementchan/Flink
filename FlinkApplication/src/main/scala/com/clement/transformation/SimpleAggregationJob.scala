package com.clement.transformation

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object SimpleAggregationJob {

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "localhost:9092")
//    properties.setProperty("group.id", "com.clement")


    // Create a dummy data stream
    val tupleStream: DataStream[(Int, Int, Int)] = env.fromElements(
      (0, 0, 0), (0, 3, 1), (0, 2, 2),
      (1, 0, 6), (1, 3, 7), (1, 2, 8)
    )


    // Result as follow:
    //  (0,0,0)
    //  (0,1,0)
    //  (0,3,0)
    //  (1,0,6)
    //  (1,1,6)
    //  (1,3,6)

    val sumStream = tupleStream.keyBy( x=> x._1 )
      .sum(1)
      .print()

    val maxStream = tupleStream.keyBy( x=> x._1 )
      .max(2)
      .print()


    val maxByStream = tupleStream.keyBy(x => x._1)
      .maxBy(2)
      .print()

    env.execute("Basic Aggregation")


  }
}
