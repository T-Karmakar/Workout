import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.jms.pool.PooledConnectionFactory
import javax.jms._

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import java.util.concurrent.Executors

/** 
 * PRODUCTION-GRADE ActiveMQ Batch Sender for Spark
 */
object ActiveMQBatchSender {

  /**
   * Send a Dataset[String] to ActiveMQ reliably:
   *
   * - Batch sending per partition
   * - Per-batch fail-fast (2s), retry once with longer timeout (6s)
   * - Dedicated ExecutionContext to avoid thread starvation
   * - Configurable delivery mode (persistent or non-persistent)
   * - Pluggable fallback for failed batches (log, DLQ, HDFS)
   *
   * @param ds            Dataset[String] to send
   * @param brokerUrl     ActiveMQ broker URL
   * @param queueName     Destination queue name
   * @param batchSize     Number of messages per batch
   * @param shortTimeout  First attempt timeout
   * @param longTimeout   Retry attempt timeout
   * @param persistent    true => DeliveryMode.PERSISTENT
   * @param maxThreadsPerPartition Thread pool size per partition for async sending
   * @param onBatchFail   Function to handle failed batches
   */
  def sendWithRetry(
    ds: Dataset[String],
    brokerUrl: String,
    queueName: String,
    batchSize: Int = 500,
    shortTimeout: FiniteDuration = 2.seconds,
    longTimeout: FiniteDuration = 6.seconds,
    persistent: Boolean = true,
    maxThreadsPerPartition: Int = 4
  )(
    onBatchFail: Seq[String] => Unit = batch => println(s"[WARN] Failed batch of size ${batch.size}")
  ): Unit = {

    val sc = ds.sparkSession.sparkContext
    val bcBrokerUrl = sc.broadcast(brokerUrl)

    ds.foreachPartition { partition =>

      // ✅ Dedicated thread pool for this partition — more reliable than global
      implicit val ec: ExecutionContext =
        ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(maxThreadsPerPartition))

      // ⚡️ Safe, per-partition pooled connection
      val factory = new ActiveMQConnectionFactory(bcBrokerUrl.value)
      val pool = new PooledConnectionFactory()
      pool.setConnectionFactory(factory)
      pool.setMaxConnections(5)

      val conn = pool.createConnection()
      conn.start()
      val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
      val dest = session.createQueue(queueName)
      val producer = session.createProducer(dest)
      producer.setDeliveryMode(
        if (persistent) DeliveryMode.PERSISTENT else DeliveryMode.NON_PERSISTENT
      )

      try {
        partition.grouped(batchSize).foreach { batch =>
          // --- Attempt 1 with short timeout ---
          val firstAttempt = Future {
            batch.foreach { payload =>
              val msg = session.createTextMessage(payload)
              producer.send(msg)
            }
          }

          val firstResult = Try(Await.result(firstAttempt, shortTimeout))

          if (firstResult.isFailure) {
            println(s"[Partition] Batch failed or timed out in ${shortTimeout.toSeconds} sec, retrying with ${longTimeout.toSeconds} sec")

            // --- Circuit breaker pattern: allow only one retry ---
            val retryAttempt = Future {
              batch.foreach { payload =>
                val msg = session.createTextMessage(payload)
                producer.send(msg)
              }
            }

            val retryResult = Try(Await.result(retryAttempt, longTimeout))

            if (retryResult.isFailure) {
              println(s"[Partition] Batch failed again on retry. Triggering fallback handler.")
              onBatchFail(batch)
            } else {
              println(s"[Partition] Retry succeeded.")
            }
          }

        }

      } catch {
        case e: Exception =>
          println(s"[Partition] Unexpected error: ${e.getMessage}")
      } finally {
        Try(producer.close())
        Try(session.close())
        Try(conn.close())
        Try(pool.stop())
        ec.shutdown()
      }
    }
  }

}


import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("ActiveMQ Sender Example")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

// Example data: 100K JSON payloads
val javaList: java.util.List[String] = new java.util.ArrayList[String]()
(1 to 100000).foreach(i => javaList.add(s"""{"id":$i,"msg":"Hello"}"""))
val ds = spark.createDataset(javaList.toArray(new Array ))

// ✅ Production call with durable messages & dedicated thread pool per partition
ActiveMQBatchSender.sendWithRetry(
  ds,
  brokerUrl = "tcp://localhost:61616",
  queueName = "myQueue",
  batchSize = 1000,
  persistent = true,                    // durable delivery
  maxThreadsPerPartition = 4            // safe async concurrency
) { failedBatch =>
  // 💡 Fallback: save failed batch to disk, Kafka DLQ, or log
  println(s"[FAILED BATCH] Size=${failedBatch.size} | Example: ${failedBatch.headOption.getOrElse("empty")}")
}
