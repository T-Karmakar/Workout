package com.tk

object WriteToFileAnotherWay extends App {
  import org.apache.spark.sql.{SparkSession, DataFrame}
  import java.io.{BufferedWriter, FileWriter}

  // 1. Create a SparkSession
  val spark = SparkSession.builder()
    .appName("Write DataFrame to File")
    .master("local[*]") // Adjust according to your cluster setup
    .getOrCreate()

  import spark.implicits._

  // 2. Assume you have a DataFrame with 4 million records and a single string column
  val data = (1 to 4000000).map(i => s"String $i").toDF("column1")
  data.printSchema()
  println("data count {}", data.count())
  // 3. Define the file path where the data will be written
  val filePath = "output_file.txt"

  // 4. Write data in batches of 100,000 records using BufferedWriter
  val batchSize = 100000

  // Open the file in append mode
  val writer = new BufferedWriter(new FileWriter(filePath, true))

  try {
    val rowCount = data.count().toInt
    val numBatches = Math.ceil(rowCount.toDouble / batchSize).toInt

    for (batchIndex <- 0 until numBatches) {
      val offset = batchIndex * batchSize
      println("offset {}", offset)
      val batchDF = data.offset(offset).limit(batchSize)

      // Collect the batch and write to file
      batchDF.collect().foreach(row => {
        writer.write(row.getString(0))  // Assumes there's only one column
        writer.newLine()
      })
      writer.flush()  // Flush after each batch
    }
  } finally {
    writer.close()  // Close the writer
  }

  // 5. Stop the Spark session
  spark.stop()

}


