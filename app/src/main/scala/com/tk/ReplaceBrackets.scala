package com.tk

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ReplaceBrackets {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ReplaceBrackets")
      .master("local[*]") // Adjust for your cluster
      .getOrCreate()

    import spark.implicits._

    val numRows = 10000000

    // Example DataFrame with a single string column containing brackets.
    val df = spark.range(numRows).map(i => s"[value$i] something else [another value$i]")

    val replacedDF = df.withColumn("replaced_string", regexp_replace(col("value"), "\\[", ""))
      .withColumn("replaced_string", regexp_replace(col("replaced_string"), "\\]", "\n"));

    replacedDF.show(5, false); //show first 5 rows of the result

    spark.stop()
  }
}
