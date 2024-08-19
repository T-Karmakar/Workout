package com.tk

//import com.tk.WriteToFileDataSetRow.data.exprEnc
import org.apache.spark.sql.DataFrame

object WriteToFileDataSetRow extends App {
  import org.apache.spark.sql.{SparkSession, Dataset, Row}
  import java.io.{BufferedWriter, FileWriter}

  // 1. Create a SparkSession
  val spark = SparkSession.builder()
    .appName("Write DataFrame to File in Batches")
    .master("local[*]") // Adjust as per your cluster setup
    .getOrCreate()

  import spark.implicits._

  // 2. Assume you have a DataFrame with 4 million records and a single string column
  //val data: Dataset[Row] = (1 to 4000000).map(i => s"String $i").toDF("column1").as[Row]
  var data: DataFrame = (1 to 4000000).map(i => s"String $i").toDF("column1")

  // 3. Define the file path where the data will be written
  val filePath = "output_file.txt"

  // 4. Write data in batches of 100,000 records using BufferedWriter
  val batchSize = 100000

  // Open the file in append mode
  val writer = new BufferedWriter(new FileWriter(filePath, true))

  // 5. Process the DataFrame in partitions
  data.foreachPartition { partition: Iterator[Row] =>
    partition.grouped(batchSize).foreach { batch =>
      // Convert the batch to a string with each record on a new line
      val stringData = batch.map(row => row.getString(0)).mkString("\n")

      // Write the batch to the file
      writer.synchronized {
        writer.write(stringData + "\n")
        writer.flush() // Ensure data is written to the file immediately
      }
    }
  }

  // Close the writer after processing
  writer.close()

  // 6. Stop the Spark session
  spark.stop()

}
