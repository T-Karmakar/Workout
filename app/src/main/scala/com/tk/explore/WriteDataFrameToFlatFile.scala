package com.tk.explore

class WriteDataFrameToFlatFile {

}

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.{BufferedWriter, FileWriter}

object WriteDataFrameToFlatFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Write DataFrame to Flat File")
      .master("local[*]")  // Adjust as per your cluster setup
      .getOrCreate()

    // Sample DataFrame (you should replace this with your actual DataFrame)
    val data = (1 to 10000000).map(i => (i, s"record_$i"))
    val df = spark.createDataFrame(data).toDF("id", "value")

    // Define batch size
    val batchSize = 100000  // Adjust the batch size based on your memory and disk capacity

    // File to write
    val outputFilePath = "output/data.txt"

    // Write DataFrame to file in batches
    writeDataFrameInBatches(df, outputFilePath, batchSize)

    // Stop SparkSession
    spark.stop()
  }

  def writeDataFrameInBatches(df: DataFrame, filePath: String, batchSize: Int): Unit = {
    // Convert DataFrame to RDD for efficient partition-wise processing
    val rdd = df.rdd

    // Initialize BufferedWriter
    val fileWriter = new BufferedWriter(new FileWriter(filePath))

    try {
      // Iterate over partitions and write to file
      rdd.foreachPartition { partition =>
        partition.grouped(batchSize).foreach { batch =>
          // Convert the batch to a string (each row separated by a newline)
          val batchData = batch.map(row => row.mkString(",")).mkString("\n")

          // Write batch data to the file
          fileWriter.synchronized {
            fileWriter.write(batchData + "\n")
          }
        }
      }
    } finally {
      // Close the BufferedWriter
      fileWriter.close()
    }
  }
}
