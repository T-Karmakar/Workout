package com.tk

class WriteLargeDataFrame {

}

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.{BufferedWriter, FileWriter}

object WriteLargeDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Write Large DataFrame")
      .config("spark.master", "local[*]")
      .getOrCreate()

    import spark.implicits._

    // Assuming df is your DataFrame with 4 million records and a single string column
    //val df: DataFrame = spark.read.text("input.txt")  // Replace with your DataFrame source
    val df = (1 to 4000000).map(i => s"String $i").toDF("column1")

    val batchSize = 100000 // Process 100,000 records at a time
    val outputFile = "output.txt"

    df.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
      val writer = new BufferedWriter(new FileWriter(outputFile, true))  // Append mode
      try {
        partition.grouped(batchSize).foreach { batch =>
          val stringData = batch.map(_.getString(0)).mkString("\n")
          writer.write(stringData + "\n")
          writer.flush()  // Ensure data is written to disk immediately to free up memory
        }
      } finally {
        writer.close()
      }
    }

    spark.stop()
  }
}
