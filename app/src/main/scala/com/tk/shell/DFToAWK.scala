package com.tk.shell

class DFToAWK {

}

import org.apache.spark.sql.{DataFrame, SparkSession}

object DFToAWK {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("DataFrame to AWK")
      .master("local[*]")  // Adjust as per your cluster setup
      .getOrCreate()

    // Sample DataFrame (replace this with your actual DataFrame)
    val data = (1 to 15000000).map(i => (i, s"record_$i", s"data_$i"))
    val df = spark.createDataFrame(data).toDF("id", "name", "value")

    // Define the .dat file path
    val outputFilePath = "output/data.dat"

    // Write DataFrame to .dat file using AWK
    writeDataFrameToDatUsingAWK(df, outputFilePath)

    // Stop SparkSession
    spark.stop()
  }

  def writeDataFrameToDatUsingAWK(df: DataFrame, filePath: String): Unit = {
    // Use foreachPartition to process each partition separately
    df.foreachPartition { partition =>
      // Convert the partition to a string
      val partitionData = partition.map(row => row.mkString(",")).mkString("\n")

      // Write the partition data to stdout and pipe it to awk
      val process = new ProcessBuilder("awk", "{print $0}", ">", filePath)
        .redirectOutput(ProcessBuilder.Redirect.appendTo(new java.io.File(filePath)))
        .start()

      // Write the data to the process's stdin
      val outputStream = process.getOutputStream
      outputStream.write(partitionData.getBytes)
      outputStream.flush()
      outputStream.close()

      // Wait for the process to finish
      process.waitFor()
    }
  }
}

