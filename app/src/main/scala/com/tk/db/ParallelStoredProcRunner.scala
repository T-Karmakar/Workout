import java.sql.{CallableStatement, Connection, DriverManager}
import java.util.concurrent.{Executors, ExecutorService, Future => JFuture}
import scala.jdk.CollectionConverters._

object ParallelStoredProcRunner {

  // MySQL connection settings
  val url = "jdbc:mysql://localhost:3306/mydb"
  val user = "root"
  val password = "mypassword"

  // Create JDBC connection
  def getConnection(): Connection = {
    DriverManager.getConnection(url, user, password)
  }

  // Method to execute stored procedure
  def runStoredProcedure(procName: String): Unit = {
    val conn: Connection = getConnection()
    try {
      val stmt: CallableStatement = conn.prepareCall(s"{call $procName()}")
      stmt.execute()
      println(s"✅ Stored procedure $procName executed successfully.")
      stmt.close()
    } catch {
      case e: Exception =>
        println(s"❌ Error executing $procName: ${e.getMessage}")
    } finally {
      conn.close()
    }
  }

  // Run multiple procedures in parallel
  def runProceduresInParallel(procs: Seq[String], threadPoolSize: Int = 4): Unit = {
    val executor: ExecutorService = Executors.newFixedThreadPool(threadPoolSize)

    try {
      // Submit all stored procedures
      val futures: Seq[JFuture[_]] = procs.map { proc =>
        executor.submit(new Runnable {
          override def run(): Unit = runStoredProcedure(proc)
        })
      }

      // Wait for all to complete
      futures.foreach(_.get())  // blocks until all finish
      println("🎉 All stored procedures finished execution.")
    } finally {
      executor.shutdown()
    }
  }

  // Demo
  def main(args: Array[String]): Unit = {
    val storedProcs = Seq("proc_sales_update", "proc_cleanup", "proc_generate_report")
    runProceduresInParallel(storedProcs, threadPoolSize = 3)
  }
}
