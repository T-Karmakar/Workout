package com.tk.explore

class SequentialWriteExample {

}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.io.{BufferedWriter, FileWriter}

object SequentialWriteExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SequentialWriteExample")
      .master("local[*]") // Adjust as per your cluster setup
      .getOrCreate()

    // Generate or Load a large DataFrame
    val df: DataFrame = spark.range(0, 4000000)
      .withColumn("value", concat(lit("record_"), col("id")))

    // Set batch size for processing (e.g., 100,000 records at a time)
    val batchSize = 100000
    val outputPath = "output/large_data.txt"

    // Write DataFrame in batches
    writeDataFrameInBatches(df, batchSize, outputPath)

    spark.stop()
  }

  def writeDataFrameInBatches(df: DataFrame, batchSize: Int, outputPath: String): Unit = {
    df.repartition(math.ceil(df.count().toDouble / batchSize).toInt) // Repartition to match batch size
      .foreachPartition { partition =>
        // Using a single BufferedWriter for each partition
        val writer = new BufferedWriter(new FileWriter(outputPath, true))

        partition.foreach { row =>
          writer.write(row.mkString(",") + "\n") // Write each row to the file
        }

        writer.close() // Close the writer to flush and release resources
      }
  }
}

