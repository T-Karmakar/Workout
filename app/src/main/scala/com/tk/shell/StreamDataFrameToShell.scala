package com.tk.shell

class StreamDataFrameToShell {

}

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.sys.process._

object StreamDataFrameToShell {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Stream DataFrame to Shell Script")
      .master("local[*]")  // Adjust according to your cluster
      .getOrCreate()

    // Sample DataFrame (replace with your actual DataFrame)
    val data = (1 to 6000000).map(i => (i, s"record_$i"))
    val df = spark.createDataFrame(data).toDF("id", "value")

    // Batch size for processing
    val batchSize = 100000  // Adjust batch size as needed

    // Write DataFrame rows to shell script in batches
    df.rdd.foreachPartition { partition =>
      partition.grouped(batchSize).foreach { batch =>
        // Convert each batch to a string (CSV format)
        //val batchData = batch.map(row => row.mkString(",")).mkString("\n")
        //val batchData = batch.map(row => row).mkString("\n")
        val batchData = batch.map(_.getString(0)).mkString("\n")

        // Pass the data to the shell script
        val process = Seq("/bin/bash", "-c", "path/to/your/script.sh")
        val output = process #< new java.io.ByteArrayInputStream(batchData.getBytes)
        output.!!
      }
    }

    spark.stop()
  }
}

