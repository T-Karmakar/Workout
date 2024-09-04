package com.tk.shell

class DataFrameToAwk {

}

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.{BufferedWriter, OutputStreamWriter}
import sys.process._

object DataFrameToAwk {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame to AWK")
      .master("local[*]")  // Adjust based on your cluster setup
      .getOrCreate()

    // Example DataFrame creation (replace this with your actual DataFrame)
    val data = (1 to 15000000).map(i => (i, s"record_$i"))
    val df = spark.createDataFrame(data).toDF("id", "value")

    // Define the AWK command (adjust as needed)
    val awkCommand = "awk '{print $1, $2}' > output.dat"

    // Use Spark to stream data to AWK
    df.rdd.foreachPartition { partition =>
      val process = new ProcessBuilder("/bin/sh", "-c", awkCommand).start()
      val writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream))

      try {
        partition.foreach { row =>
          writer.write(s"${row.getInt(0)},${row.getString(1)}\n")
        }
      } finally {
        writer.close()
        process.waitFor()
      }
    }

    spark.stop()
  }
}

