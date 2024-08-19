package com.tk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{SparkSession, Row}
import java.io.{BufferedWriter, FileWriter}

object WriteToFIleScalaRDDWay extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder()
    .appName("DataFrame Offset Example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  /*// Assume we have a DataFrame 'df'
  //val df = spark.read.option("header", "true").csv("path/to/your/csv")
  val df : DataFrame = (1 to 4000000).map(i => s"String $i").toDF("column1")

  // Convert DataFrame to RDD and skip the first N rows
  val offset = 100000
  val rddWithIndex = df.rdd.zipWithIndex()
  val filteredRDD = rddWithIndex.filter { case (row, index) => index >= offset }.map(_._1)

  // Convert back to DataFrame
  val dfWithOffset = spark.createDataFrame(filteredRDD, df.schema)

  // Show the DataFrame with the offset applied
  dfWithOffset.show()*/


  // 2. Create a DataFrame with 4 million records and a single string column
  val data = (1 to 4000000).map(i => s"String $i").toDF("column1")

  // Convert DataFrame to RDD
  var rdd = data.rdd

  // 3. Define the file path where the data will be written
  val filePath = "output_file.txt"

  // 4. Set batch size
  val batchSize = 100000

  // Open the file in append mode
  val writer = new BufferedWriter(new FileWriter(filePath, true))

  try {
    while (!rdd.isEmpty()) {
      // 5. Take the first batch from the RDD
      val batch = rdd.take(batchSize)

      // 6. Write the batch to the file
      val stringData = batch.map(_.getString(0)).mkString("\n")
      writer.write(stringData + "\n")
      writer.flush() // Ensure data is written to the file immediately

      // 7. Remove the processed batch from the RDD
      val batchRDD = spark.sparkContext.parallelize(batch)
      rdd = rdd.subtract(batchRDD)
    }
  } finally {
    // Close the writer after processing
    writer.close()
  }

  // 8. Stop the Spark session
  spark.stop()


}
