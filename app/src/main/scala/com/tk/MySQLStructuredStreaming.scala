package com.tk

class MySQLStructuredStreaming {

}


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import java.sql.Timestamp

object MySQLStructuredStreaming {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder
      .appName("Spark Structured Streaming with MySQL")
      .master("local[*]")  // For local testing, use * to use all cores
      .getOrCreate()

    import spark.implicits._

    // JDBC connection properties
    val jdbcUrl = "jdbc:mysql://localhost:3306/testdb"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "root")
    connectionProperties.setProperty("password", "your_password")
    connectionProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    // Reading data from MySQL table using Structured Streaming
    val inputDF = spark.readStream
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "input_table")
      .option("user", "root")
      .option("password", "your_password")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .load()

    // Processing the data
    val processedDF = inputDF
      .selectExpr("id", "name", "age", "current_timestamp() as processed_at")
      .as[(Int, String, Int, Timestamp)]

    // Writing the processed data to MySQL
    val query = processedDF.writeStream
      .foreachBatch { (batchDF, batchId) =>
        batchDF.write
          .mode("append")
          .jdbc(jdbcUrl, "output_table", connectionProperties)
      }
      .trigger(Trigger.ProcessingTime("10 seconds")) // Trigger every 10 seconds
      .outputMode("append")  // Since we're appending the data
      .start()

    // Start the query and wait for its termination
    query.awaitTermination()
  }
}
