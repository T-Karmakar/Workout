package com.tk.shell

class WriteDataFrameWithSed {

}

import org.apache.spark.sql.{DataFrame, SparkSession}
import sys.process._

object WriteDataFrameWithSed {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Write DataFrame with Sed")
      .master("local[*]")  // Adjust this for your cluster setup
      .getOrCreate()

    // Sample DataFrame (replace with your actual DataFrame)
    import spark.implicits._
    val df = (1 to 6000000).map(i => (i, s"sample_data_$i")).toDF("id", "value")

    // File path to write
    val filePath = "path/to/your/output.dat"

    // Write the DataFrame to file using sed
    writeDataFrameWithSed(df, filePath)

    spark.stop()
  }

  def writeDataFrameWithSed(df: DataFrame, filePath: String): Unit = {
    // Convert DataFrame rows to a single string with newline separation
    df.foreachPartition { partition =>
      val data = partition.map(row => row.mkString(",")).mkString("\n")

      // Define the sed command to write to the file
      val command = Seq("/bin/bash", "-c", s"sed '' > $filePath")

      // Pass the data to sed
      val output = command #< new java.io.ByteArrayInputStream(data.getBytes)
      output.!!
    }
  }
}

