package com.sap.kafka.connect.sink

import java.sql.SQLException
import java.util

import com.sap.kafka.hanaClient.HANAJdbcClient
import com.sap.kafka.connect.config.{HANAConfig, Parameters}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import org.slf4j.{Logger, LoggerFactory}


class HANASinkTask extends SinkTask {
    /**
     * Parse the configurations and setup the writer
     * */
    private val log: Logger = LoggerFactory.getLogger(classOf[HANASinkTask])
    private var writer:HANAWriter = null
    private var retriesLeft:Int = 0
    private var config:HANAConfig = null

    var hanaClient: HANAJdbcClient = _


    override def start(props: util.Map[String, String]): Unit = {
      log.info("Starting Kafka-Connect task")
      config = Parameters.getConfig(props)
      hanaClient = new HANAJdbcClient(config)
      initWriter(config)
    }

  def initWriter(config: HANAConfig): Unit = {
    writer = new HANAWriter(config, hanaClient)
  }

    /**
     * Pass the SinkRecords to the writer for Writing
     * */
    @throws(classOf[SQLException])
    override def put(records: util.Collection[SinkRecord]): Unit = {
      retriesLeft = config.maxRetries
      if (records.isEmpty) {
        return
      }
      val recordsCount: Int = records.size
      log.trace("Received {} records for Sink", recordsCount)
      try {
        writer.write(records)
      } catch  {
        case sqlException : SQLException =>
          log.error("Write of {} records failed, remainingRetries={}", records.size(), retriesLeft)
      if (retriesLeft == 0) {
        throw new ConnectException(sqlException)
      } else {
        retriesLeft = retriesLeft -1
        writer.close()
        writer = new HANAWriter(config, hanaClient)
        writer.write(records)
      }
    }
      retriesLeft = config.maxRetries
    }


    override def stop(): Unit = {
      log.info("Stopping task")
      writer.close()
    }

    override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]) : Unit = {




    }

    override def version(): String = getClass.getPackage.getImplementationVersion
}
