package com.tk.db

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.sql.{Connection, DriverManager, PreparedStatement}

class DataBatchInsert {
  object DataBatchInsert {
    private val spark = SparkSession.builder()
      .appName("CSV to MySQL Bulk Insert")
      .config("spark.sql.shuffle.partitions", "200") // Adjust based on cores/memory
      .getOrCreate()

    // Adjust the schema or use header = true if CSV has column names
    private val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/path/to/your/70_million_file.csv")

    // Repartition for parallelism (match cluster capacity)
    private val partitionedDF = df.repartition(100)

    // JDBC & SQL Setup
    val jdbcUrl = "jdbc:mysql://<host>:<port>/<db>?rewriteBatchedStatements=true"
    val user = "yourUser"
    val password = "yourPassword"
    private val insertSQL = "INSERT INTO your_table(col1, col2, col3) VALUES (?, ?, ?)"

    // Efficient insert per partition
    partitionedDF.foreachPartition { partition: Iterator[Row] =>
      var connection: Connection = null
      var statement: PreparedStatement = null

      try {
        connection = DriverManager.getConnection(jdbcUrl, user, password)
        connection.setAutoCommit(false)
        statement = connection.prepareStatement(insertSQL)

        val batchSize = 5000
        var count = 0

        partition.foreach { row =>
          statement.setString(1, row.getAs[String]("col1"))
          statement.setInt(2, row.getAs[Int]("col2"))
          statement.setDouble(3, row.getAs[Double]("col3"))

          statement.addBatch()
          count += 1

          if (count % batchSize == 0) {
            statement.executeBatch()
            connection.commit()
          }
        }

        // Final batch
        if (count % batchSize != 0) {
          statement.executeBatch()
          connection.commit()
        }

      } catch {
        case e: Exception =>
          println(s"Batch insert failed: ${e.getMessage}")
          if (connection != null) connection.rollback()
      } finally {
        if (statement != null) statement.close()
        if (connection != null) connection.close()
      }
    }
  }
}
