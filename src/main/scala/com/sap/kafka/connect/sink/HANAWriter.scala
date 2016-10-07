package com.sap.kafka.connect.sink

import java.sql.{Connection, SQLException}
import java.util

import com.sap.kafka.hanaClient.HANAClientException
import com.sap.kafka.hanaClient.HANAJdbcClient
import com.sap.kafka.connect.config.HANAConfig
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._


class HANAWriter(config: HANAConfig,
                 hanaClient: HANAJdbcClient) {

  private val log: Logger = LoggerFactory.getLogger(getClass)
  private var connection:Connection = null

  @throws(classOf[HANAClientException])
  private def initializeHANAConnection(): Unit = {
    if(connection == null || connection.isClosed ) {
      connection = hanaClient.getConnection
    }
    else if(!connection.isValid(120))
    {
      connection.close()
      connection = hanaClient.getConnection
    }
    connection.setAutoCommit(false)
  }


  @throws(classOf[SQLException])
  private[sink] def write(records: util.Collection[SinkRecord]): Unit = {

    try{
      initializeHANAConnection()
    }
    catch {
      case e:HANAClientException => log.error(s"Error Connecting to HANA Instance : ${e.getMessage}")
        throw new SQLException(e.getMessage,e.getCause)
    }

    val tableCache = scala.collection.mutable.Map[String, HANASinkRecordsCollector]()
    records.foreach(record => {
      var table = config.topicProperties(record.topic()).get("table.name").get
      if (table.contains("${topic}")) {
        table = table.replace("${topic}", record.topic())
      }

      val recordsCollector: Option[HANASinkRecordsCollector] = tableCache.get(table)
      recordsCollector match {
        case None =>
          val tableRecordsCollector = new HANASinkRecordsCollector(table, hanaClient, connection, config)
          tableCache.put(table, tableRecordsCollector)
          tableRecordsCollector.add(record)
        case Some(tableRecordsCollector) =>
          tableRecordsCollector.add(record)
      }
    })
    flush(tableCache.toMap)
  }

  private def flush(tableCache: Map[String, HANASinkRecordsCollector]): Unit = {
    for ((table, recordsCollector) <- tableCache) {
        try {
          recordsCollector.flush()
        } catch {
          case e: Exception => log.error(s"Error in flushing data for table $table : ${e.getMessage}")
            throw new SQLException(e.getMessage,e.getCause)
        }
    }
    connection.commit()
  }

  private[sink] def close(): Unit = {
    if (connection != null) {
      try {
        connection.close()
        connection = null
      }
      catch {
        case sqle: SQLException => log.warn("Ignoring error closing connection", sqle)
      }
    }
  }
}
