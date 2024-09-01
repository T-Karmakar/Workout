package com.tk.streaming

class SparkStructuredStreamingExample {

}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object SparkStructuredStreamingExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark Structured Streaming Example")
      .master("local[*]") // Adjust as per your cluster setup
      .getOrCreate()

    import spark.implicits._

    // MySQL Database configurations
    val jdbcUrl = "jdbc:mysql://localhost:3306/testdb"
    val dbTable = "test_table"
    val dbUser = "root"
    val dbPassword = "password"

    // Example 1: Read from MySQL database and write to a CSV file
    val mysqlDF = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", dbTable)
      .option("user", dbUser)
      .option("password", dbPassword)
      .load()

    mysqlDF.write
      .option("header", "true")
      .csv("output/mysql_to_csv")

    // Example 2: Read from a CSV file and write to a MySQL database
    val csvDF = spark.read.option("header", "true")
      .csv("input/sample.csv")

    csvDF.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", dbTable)
      .option("user", dbUser)
      .option("password", dbPassword)
      .mode("append")
      .save()

    // Example 3: Streaming from a CSV file and writing to MySQL database
    val streamingCSVDF = spark.readStream
      .option("header", "true")
      .csv("input/streaming_csv")

    val query1 = streamingCSVDF.writeStream
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", dbTable)
      .option("user", dbUser)
      .option("password", dbPassword)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Example 4: Streaming from MySQL database and writing to a CSV file
    val streamingMySQLDF = spark.readStream.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", dbTable)
      .option("user", dbUser)
      .option("password", dbPassword)
      .load()

    val query2 = streamingMySQLDF.writeStream
      .format("csv")
      .option("header", "true")
      //.csv("output/streaming_mysql_to_csv")
      .option("path", "output/streaming_mysql_to_csv")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Await termination for both streams
    query1.awaitTermination()
    query2.awaitTermination()

    // Stop the Spark session
    spark.stop()
  }
}

