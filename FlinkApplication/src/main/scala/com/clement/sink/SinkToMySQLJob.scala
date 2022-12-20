/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clement.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.sql.PreparedStatement
import java.util.Properties


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */


object SinkToMySQLJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "com.clement")

    val statementBuilder : JdbcStatementBuilder[String] =
      new JdbcStatementBuilder[String] {
        override def accept(t: PreparedStatement, u: String): Unit = {
          t.setString(1,u + "_proceed")
        }
      }

    // create the execution options for the JDBC
    val executionOptions = JdbcExecutionOptions.builder()
                          .withBatchSize(1000)
                          .withBatchIntervalMs(200)
                          .withMaxRetries(5)
                          .build()

    // create the connection
    val connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                          .withUrl("jdbc:mysql://localhost:3306/demo")
                          .withDriverName("com.mysql.jdbc.Driver")
                          .withUsername("root")
                          .withPassword("1234")
                          .build()

    // To get data from the stream
    val stream = env
      .addSource(new FlinkKafkaConsumer[String]("quickstart-events", new SimpleStringSchema(), properties))
      .map { x => x }


    val sinkToMySQL = JdbcSink.sink(
                      "insert into test (text) values (?)",
                      statementBuilder,
                      executionOptions,
                      connectionOptions
                    )

    stream.addSink(sinkToMySQL)


    env.execute("Flink Streaming Scala API Skeleton")

  }
}
