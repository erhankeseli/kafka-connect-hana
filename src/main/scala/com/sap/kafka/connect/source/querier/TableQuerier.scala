package com.sap.kafka.connect.source.querier

import com.sap.kafka.hanaClient.HANAJdbcClient
import com.sap.kafka.connect.config.HANAConfig
import com.sap.kafka.connect.source.querier.QueryMode.QueryMode
import com.sap.kafka.utils.JdbcTypeConverter
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.slf4j.LoggerFactory

object QueryMode extends Enumeration {
  type QueryMode = Value
  val TABLE = Value
}

abstract class TableQuerier(mode: QueryMode, table: String,
                            topic: String, hanaSourceConfig: HANAConfig,
                            var hanaJdbcClient: Option[HANAJdbcClient])
                extends Comparable[TableQuerier] {
  var tableName: String = if (mode.equals(QueryMode.TABLE)) table else null
  var lastUpdate: Long = 0
  var schema: Schema = _
  var queryString: Option[String] = None
  var resultList: Option[List[Struct]] = None

  val log = LoggerFactory.getLogger(getClass)

  def getLastUpdate(): Long = lastUpdate

  def getOrCreateQueryString(): Option[String] = {
    createQueryString()
    queryString
  }

  def createQueryString(): Unit

  def querying(): Boolean = resultList.isDefined

  def maybeStartQuery(): Unit = {
    if (resultList.isEmpty) {
      val metadata = getOrCreateHanaJdbcClient().get.getMetaData(table, None)
      schema = JdbcTypeConverter.convertHANAMetadataToSchema(tableName, metadata)
      queryString = getOrCreateQueryString()

      val batchMaxRows = hanaSourceConfig.batchMaxRows
      resultList = getOrCreateHanaJdbcClient().get.executeQuery(schema, queryString.get,
        0, batchMaxRows)
      log.info(resultList.size.toString)
    }
  }

  def extractRecords(): List[SourceRecord]

  def close(now: Long): Unit = {
    resultList = None
    schema = null

    lastUpdate = now
  }

  protected def getOrCreateHanaJdbcClient(): Option[HANAJdbcClient] = {
    if (hanaJdbcClient.isDefined) {
      return hanaJdbcClient
    }
    hanaJdbcClient = Some(HANAJdbcClient(hanaSourceConfig))
    hanaJdbcClient
  }

  override def compareTo(other: TableQuerier): Int = {
    if (this.lastUpdate < other.lastUpdate) {
      -1
    } else if (this.lastUpdate > other.lastUpdate) {
      0
    } else {
      this.tableName.compareTo(other.tableName)
    }
  }
}