package com.tk.activemq

import jakarta.jms.Session
import org.apache.activemq.ActiveMQSslConnectionFactory
import org.apache.activemq.jms.pool.PooledConnectionFactory

import javax.jms._
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

class ActiveMQProducerHelper(brokerUrl: String, queueName: String, threadPoolSize: Int = 4) extends Serializable {

  // Method to send a batch of messages from a partition
  def sendMessages(partition: Iterator[String]): Unit = {
    // Create connection factory per partition (inside executor)
    val connFactory = new ActiveMQSslConnectionFactory(brokerUrl)
    val factory = new PooledConnectionFactory()
    factory.setConnectionFactory(connFactory)
    factory.setMaxConnections(threadPoolSize)

    val connection = factory.createConnection()
    connection.start()

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val destination = session.createQueue(queueName)
    val producer = session.createProducer(destination)

    val pool: ExecutorService = Executors.newFixedThreadPool(threadPoolSize)

    try {
      val messages = partition.toList

      val tasks = messages.map { msg =>
        new Runnable {
          override def run(): Unit = {
            try {
              val textMsg = session.createTextMessage(msg)
              producer.send(textMsg)
            } catch {
              case e: Exception =>
                System.err.println(s"Message send failed: ${e.getMessage}")
                e.printStackTrace()
            }
          }
        }
      }

      tasks.foreach(pool.submit)
      pool.shutdown()
      pool.awaitTermination(10, TimeUnit.MINUTES)
    } finally {
      producer.close()
      session.close()
      connection.close()
      factory.stop()
    }
  }
}
