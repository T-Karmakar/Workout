package com.tk

import com.tk.WriteToFileDataSetRow.data.exprEnc
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.io.{BufferedWriter, FileWriter}

object WriteToFileDataSet extends App {

    // 1. Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Write DataFrame in Batches")
      .master("local[*]") // Adjust as per your cluster setup
      .getOrCreate()

    import spark.implicits._

    // 2. Create a DataFrame with 4 million records and a single string column
    var data: Dataset[Row] = (1 to 4000000).map(i => s"String $i").toDF("column1").as[Row]

    // 3. Define the file path where the data will be written
    val filePath = "output_file.txt"

    // 4. Set batch size
    val batchSize = 100000

    // Open the file in append mode
    val writer = new BufferedWriter(new FileWriter(filePath, true))

    try {
      while (!data.isEmpty) {
        // 5. Select the first batch from the DataFrame
        val batch = data.limit(batchSize).collect()  // Collect the batch as an array of Rows

        // 6. Remove the selected batch from the main DataFrame
        val batchIds = batch.map(row => row.getString(0))
        data = data.filter(row => !batchIds.contains(row.getString(0)))

        // 7. Write the batch to the file
        val stringData = batch.map(_.getString(0)).mkString("\n")
        writer.write(stringData + "\n")
        writer.flush() // Ensure data is written to the file immediately
      }
    } finally {
      // Close the writer after processing
      writer.close()
    }

    // 8. Stop the Spark session
    spark.stop()
}