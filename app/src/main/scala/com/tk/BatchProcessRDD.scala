package com.tk

import org.apache.spark.sql.SparkSession
import java.io.{BufferedWriter, FileWriter}

object BatchProcessRDD extends App {

  // 1. Create a SparkSession
  /*val spark = SparkSession.builder()
    .appName("Batch Process RDD with Resource Management")
    .master("local[*]") // Adjust based on your environment
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "4g")
    .config("spark.memory.fraction", "0.6")
    .config("spark.memory.storageFraction", "0.3")
    .config("spark.local.dir", "/path/to/tmp") // Ensure this path has sufficient space
    .getOrCreate()*/

  val spark = SparkSession.builder()
    .appName("DataFrame Offset Example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // 2. Create an RDD with 4 million records
  //var rdd = spark.sparkContext.parallelize(1 to 4000000).map(i => s"String $i")
  val data = (1 to 4000000).map(i => s"String $i").toDF("column1")
  private var rddData = data.rdd
  private var rddDataIndex = rddData.zipWithIndex()
  // 3. Define batch size (adjusted for memory constraints)
  val batchSize = 100000

  // 4. Define the file path where the data will be written
  val filePath = "output_file.txt"

  // Open the file in append mode
  val writer = new BufferedWriter(new FileWriter(filePath, true))

  val rowCount = rddData.count().toInt
  val numBatches = Math.ceil(rowCount.toDouble/batchSize).toInt

  try {
    /*while (!rdd.isEmpty()) {
      // 5. Process a batch
      val batch = rdd.take(batchSize)

      // 6. Remove the processed batch from the RDD
      val batchRDD = spark.sparkContext.parallelize(batch)
      rdd = rdd.subtract(batchRDD)

      // 7. Write the batch to a file
      val stringData = batch.mkString("\n")
      writer.write(stringData + "\n")
      writer.flush() // Ensure data is written to the file immediately

      // Clean up to avoid filling up disk space
      spark.catalog.clearCache()
      System.gc()
    }*/
    for (i <- 0L until (rddData.count() / batchSize)) {
    //for (i <- 0L until batchSize) {
      val batchRDD = rddDataIndex.filter {
        case (_, index) => index >= i * batchSize && index < (i + 1) * batchSize
      }.map(_._1) // Remove index
      // Process batchRDD
      val stringData = batchRDD.collect().mkString("\n")
      writer.write(stringData + "\n")
      writer.flush()

      spark.catalog.clearCache()
      System.gc()
    }
  } finally {
    // Close the writer after processing
    writer.close()
  }

  // 8. Stop the Spark session
  spark.stop()
}