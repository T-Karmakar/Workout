package com.tk

import org.apache.spark.sql.DataFrame

object WriteToFileAndSubtractProcessed extends App {
  import org.apache.spark.sql.{SparkSession, Dataset, Row}
  import org.apache.spark.sql.functions.col
  import java.io.{BufferedWriter, FileWriter}

  // 1. Create a SparkSession
  val spark = SparkSession.builder()
    .appName("Write DataFrame in Batches and Remove Batch")
    .master("local[*]")  // Adjust for your cluster setup
    .getOrCreate()

  import spark.implicits._

  // 2. Assume you have a DataFrame with 4 million records and a single string column
  //var data: Dataset[Row] = (1 to 4000000).map(i => s"String $i").toDF("column1").as[Row]
  var data: DataFrame = (1 to 4000000).map(i => s"String $i").toDF("column1")

  // 3. Define the file path where the data will be written
  val filePath = "output_file.txt"

  // 4. Define the batch size
  val batchSize = 100000

  // Open the file in append mode
  val writer = new BufferedWriter(new FileWriter(filePath, true))

  try {
    // 5. Loop to process the DataFrame in batches
    while (data.count() > 0) {
      // Get the first batch of rows
      val batch = data.limit(batchSize)

      // Convert the batch to a string and write to file
      val stringData = batch.collect().map(row => row.getString(0)).mkString("\n")
      writer.write(stringData + "\n")
      writer.flush() // Ensure data is written to the file

      // 6. Remove the processed batch from the DataFrame
      val idsToRemove = batch.withColumn("id", col("column1"))
      data = data.except(idsToRemove)
    }
  } finally {
    // Close the writer after processing
    writer.close()
  }

  // 7. Stop the Spark session
  spark.stop()

}