package com.tk

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.{BufferedWriter, File, FileWriter}

class FlatFileWrite {

}

object FlatFileWrite extends App {
  private val spark: SparkSession = SparkSession.builder()
    .appName("Write DataFrame to Single File")
    .master("local[*]")
    .getOrCreate()

  import org.apache.spark.sql.functions._

  private val numRecords = 1000000
  private val df: DataFrame = spark.range(numRecords)
    .withColumn("value", monotonically_increasing_id())
    .withColumn("data", lit("Sample data"))

  // Show the schema for verification
  df.printSchema()

  // Show a few records
  df.show(5)

  import scala.util.Using

  // Function to convert a DataFrame row to a string (CSV format for simplicity)
  private def rowToString(row: org.apache.spark.sql.Row): String = {
    row.mkString(",")
    //row.mkString
  }

  // Path to the output file
  private val outputPath = "data.csv"
  private val batchSize = 100000
  private val numBatches = (numRecords / batchSize).toInt

  // Initialize the BufferedWriter
  Using(new BufferedWriter(new FileWriter(new File(outputPath)))) { writer =>

    // Iterate over each batch
    for (batchId <- 0 until numBatches) {
      val startIdx = batchId * batchSize
      val endIdx = math.min(startIdx + batchSize, numRecords)

      // Filter the DataFrame for the current batch
      val batchDF = df.filter(col("id").between(startIdx, endIdx - 1))

      // Collect the batch data as rows
      val rows = batchDF.collect()

      // Write each row to the file
      rows.foreach(row => {
        writer.write(rowToString(row))
        writer.newLine()
      })

      // Optional: Flush the writer to ensure data is written to the file
      writer.flush()
    }

  }.getOrElse(throw new RuntimeException("Error while writing to file"))

}