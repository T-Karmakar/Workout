package com.tk

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.{BufferedWriter, FileWriter}
import scala.jdk.CollectionConverters.IterableHasAsJava

object BatchProcessingWithOffset extends App {

  // 1. Create a SparkSession
  val spark = SparkSession.builder()
    .appName("Batch Processing with Offset")
    .master("local[*]") // Adjust as per your cluster setup
    .getOrCreate()

  import spark.implicits._

  // 2. Create a DataFrame with 4 million records and a single string column
  var data: DataFrame = (1 to 4000000).map(i => s"String $i").toDF("column1")

  // 3. Define the file path where the data will be written
  val filePath = "output_file.txt"

  // 4. Set batch size
  val batchSize = 100000

  // Open the file in append mode
  val writer = new BufferedWriter(new FileWriter(filePath, true))

  var offset = 0

  try {
    while (!data.isEmpty) {
     /* // 5. Get the current batch
      val batch = data.limit(batchSize).collect() // Collect the batch as an array of Rows

      // 6. Write the batch to the file
      val stringData = batch.map(_.getString(0)).mkString("\n")
      writer.write(stringData + "\n")
      writer.flush() // Ensure data is written to the file immediately

      // 7. Remove the processed batch from the DataFrame
      val batchDF = spark.createDataFrame(batch.toList.asJava, data.schema)
      data = data.except(batchDF)*/

      // 5. Process a batch
      val batch = data.limit(batchSize)  // Select the first batch of records

      // 6. Write the batch to a file
      val stringData = batch.collect().map(_.getString(0)).mkString("\n")
      writer.write(stringData + "\n")
      writer.flush() // Ensure data is written to the file immediately

      // 7. Remove the processed batch from the DataFrame
      val batchIds = batch.collect().map(row => row.getString(0))
      data = data.except(batch)  // Exclude the processed batch from the remaining DataFrame

      // Increment offset
      offset += batchSize
    }
  } finally {
    // Close the writer after processing
    writer.close()
  }

  // 8. Stop the Spark session
  spark.stop()
}
