package com.tk

class WriteDataFrameToFile {

}

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.{BufferedWriter, FileWriter}

object WriteDataFrameToFile {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("WriteDataFrameToFile")
      .master("local[*]")
      .getOrCreate()

    // Create a DataFrame with 4 million rows of dummy data
    val data = (1 to 4000000).map(i => s"String_$i")
    val df: DataFrame = spark.createDataFrame(data.map(Tuple1(_))).toDF("stringColumn")

    // Specify the output file path
    val outputPath = "file.csv"

    // Open a BufferedWriter
    val fileWriter = new FileWriter(outputPath, true) // true to append to the file if it exists
    val bufferedWriter = new BufferedWriter(fileWriter)

    // Define batch size
    val batchSize = 100000

    // Write DataFrame to file in batches
    df.foreachPartition { partition =>
      partition.grouped(batchSize).foreach { batch =>
        val stringData = batch.map(_.getString(0)).mkString("\n")
        bufferedWriter.write(stringData + "\n")
      }
    }

    // Close the BufferedWriter
    bufferedWriter.close()

    // Stop the SparkSession
    spark.stop()
  }
}
