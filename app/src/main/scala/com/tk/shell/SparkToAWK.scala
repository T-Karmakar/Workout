package com.tk.shell

class SparkToAWK {

}

import org.apache.spark.sql.{DataFrame, SparkSession}
import sys.process._

object SparkToAWK {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark to AWK")
      .master("local[*]")  // Adjust this for your cluster
      .getOrCreate()

    // Example DataFrame with 15 million records
    val data = (1 to 15000000).map(i => (i, s"record_$i"))
    val df = spark.createDataFrame(data).toDF("id", "value")

    // Use Spark to write the DataFrame to a text file or pipe
    df.rdd.map(row => row.mkString(",")).pipeToAWK()

    // Convert to RDD and use pipe to pass to awk
    //concatenatedDF.rdd.map(_.getString(0)).pipe("awk '{print $1, $2}' > " + outputPath).collect()

    // Stop the SparkSession
    spark.stop()
  }

  implicit class RDDOps(rdd: org.apache.spark.rdd.RDD[String]) {
    def pipeToAWK(): Unit = {
      // Use the pipe to stream data to awk
      val awkCommand = """awk '{print $0}' > output.dat"""  // Modify the awk command as needed
      rdd.toLocalIterator.foreach(line => {
        s"echo $line" #| awkCommand !!
      })
    }
  }
}
