import java.io.{FileWriter, PrintWriter}
import scala.io.Source
import java.nio.file.{Files, Paths, StandardCopyOption}

def writeDatFile(header: String, df: org.apache.spark.sql.DataFrame, footer: String, outputFile: String): Unit = {
  // Step 1: Write content to a temp single file
  val tmpPath = "/tmp/spark_content"
  df.coalesce(1).write.mode("overwrite").option("header", "false").csv(tmpPath)

  // Locate Spark’s single part file
  val contentFile = Files.list(Paths.get(tmpPath))
    .toArray
    .map(_.toString)
    .find(_.contains("part-"))
    .get

  // Step 2: Open target file in append mode
  val pw = new PrintWriter(new FileWriter(outputFile, true)) // true = append
  try {
    // Header only if file is new
    if (!Files.exists(Paths.get(outputFile)) || Files.size(Paths.get(outputFile)) == 0) {
      pw.println(header)
    }

    // Append content
    Source.fromFile(contentFile).getLines().foreach(pw.println)

    // Append footer at the very end only (depends on your workflow)
    pw.println(footer)
  } finally {
    pw.close()
  }
}


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import java.io.{FileWriter, PrintWriter}
import java.nio.file.{Files, Paths}
import scala.io.Source

object DatFileWriter {
  
  private var writer: PrintWriter = _

  /** Initialize writer and write header (only once) */
  def startFile(filePath: String, header: String): Unit = {
    if (writer == null) {
      writer = new PrintWriter(new FileWriter(filePath, false)) // overwrite existing
      writer.println(header)
      writer.flush()
    }
  }

  /** Append a DataFrame as content */
  def appendDataFrame(df: org.apache.spark.sql.DataFrame): Unit = {
    require(writer != null, "File not initialized. Call startFile first.")

    // Write DF content to a temp single file
    val tmpPath = "/tmp/spark_content_" + System.nanoTime()
    df.coalesce(1).write.mode("overwrite").option("header", "false").csv(tmpPath)

    // Get Spark’s part file
    val contentFile = Files.list(Paths.get(tmpPath))
      .toArray
      .map(_.toString)
      .find(_.contains("part-"))
      .get

    // Stream into the main .dat file
    Source.fromFile(contentFile).getLines().foreach(writer.println)
    writer.flush()
  }

  /** Write footer and close writer (only once, at the end) */
  def closeFile(footer: String): Unit = {
    require(writer != null, "File not initialized. Call startFile first.")
    writer.println(footer)
    writer.flush()
    writer.close()
    writer = null
  }
}



import spark.implicits._

// Example dataframes
val df1 = Seq((1,"Alice"), (2,"Bob")).toDF("id","name")
val df2 = Seq((3,"Charlie")).toDF("id","name")

val outputFile = "/tmp/output.dat"
val header = "HDR|20250922|SYSTEMX"
val footer = "TRL|000123"   // e.g. record count, checksum

// Start file once
DatFileWriter.startFile(outputFile, header)

// Append multiple DataFrames
DatFileWriter.appendDataFrame(df1)
DatFileWriter.appendDataFrame(df2)

// Close file once
DatFileWriter.closeFile(footer)

