package com.sap.kafka.connect.source

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import com.sap.kafka.hanaClient.{HANAClientException, HANAJdbcClient}
import com.sap.kafka.connect.config.{HANAConfig, Parameters}
import com.sap.kafka.connect.source.querier.{BulkTableQuerier, IncrColTableQuerier, QueryMode, TableQuerier}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

class HANASourceTask extends SourceTask {
  private var config: HANAConfig = _
  val tableQueue = new mutable.Queue[TableQuerier]()
  var time: Time = new SystemTime()
  var stopFlag: AtomicBoolean = _
  var hanaJdbcClient: HANAJdbcClient = _

  def this(time: Time, jdbcClient: HANAJdbcClient) = {
    this()
    this.time = time
    this.hanaJdbcClient = jdbcClient
  }

  val log = LoggerFactory.getLogger(getClass)

  override def version(): String = {
    getClass.getPackage.getImplementationVersion
  }

  override def start(props: util.Map[String, String]): Unit = {
    try {
      config = Parameters.getConfig(props)
    } catch {
      case e: ConfigException => throw new ConnectException("Couldn't start HANASourceTask due to configuration error", e)
    }

    if (hanaJdbcClient == null) {
      hanaJdbcClient = new HANAJdbcClient(config)
    }

    val topics = config.topics

    val tables = topics.map(topic =>
      (config.topicProperties(topic)("table.name"), topic))

    if (tables.isEmpty) {
      throw new ConnectException("Invalid configuration: each HANASourceTask must have" +
        " one table assigned to it")
    }

    // default the query mode to table.
    val queryMode = QueryMode.TABLE
    val tableInfos = getTables(tables)


    val mode = config.mode
    var offsets: util.Map[util.Map[String, String], util.Map[String, Object]] = null
    var incrementingCols: List[String] = List()

    if (mode.equals(Parameters.MODE_INCREMENTING)) {
      val partitions =
        new java.util.ArrayList[java.util.Map[String, String]](tables.length)

      queryMode match {
        case QueryMode.TABLE =>
          tableInfos.foreach(table => {
            val partition = new util.HashMap[String, String]()
            partition.put(HANASourceConnectorConstants.TABLE_NAME_KEY, table._3)
            partitions.add(partition)
            incrementingCols :+= config.topicProperties(table._4)("incrementing.column.name")
          })
      }
      offsets = context.offsetStorageReader().offsets(partitions)
    }

    var count = 0
    tableInfos.foreach(tableInfo => {
      val partition = new util.HashMap[String, String]()
      queryMode match {
        case QueryMode.TABLE =>
          partition.put(HANASourceConnectorConstants.TABLE_NAME_KEY, tableInfo._3)
        case _ =>
          throw new ConfigException(s"Unexpected Query Mode: $queryMode")
      }
      val offset = if (offsets == null) null else offsets.get(partition)

      val topic = tableInfo._4

      if (mode.equals(Parameters.MODE_BULK)) {
        tableQueue += new BulkTableQuerier(queryMode, tableInfo._1, tableInfo._2, topic,
          config, Some(hanaJdbcClient))
      } else if (mode.equals(Parameters.MODE_INCREMENTING)) {
        tableQueue += new IncrColTableQuerier(queryMode, tableInfo._1, tableInfo._2, topic,
          incrementingCols(count),
          if (offset == null) null else offset.asScala.toMap,
          config, Some(hanaJdbcClient))
      }
      count = count + 1
    })
    stopFlag = new AtomicBoolean(false)
  }

  override def stop(): Unit = {
    if (stopFlag != null) {
      stopFlag.set(true)
    }
  }

  override def poll(): util.List[SourceRecord] = {
    val topic = config.topics.head

    var now = time.milliseconds()

    while (!stopFlag.get()) {
      val querier = tableQueue.head

      var waitFlag = false
      if (!querier.querying()) {
        val nextUpdate = querier.getLastUpdate() +
          config.topicProperties(topic).get("poll.interval.ms").get.toInt
        val untilNext = nextUpdate - now

        if (untilNext > 0) {
          log.info(s"Waiting $untilNext ms to poll from ${querier.toString}")
          waitFlag = true
          time.sleep(untilNext)
          now = time.milliseconds()
        }
      }

      if (!waitFlag) {
        var results = List[SourceRecord]()

        log.info(s"Checking for the next block of results from ${querier.toString}")
        querier.maybeStartQuery()

        results ++= querier.extractRecords()

        log.info(s"Closing this query for ${querier.toString}")
        val removedQuerier = tableQueue.dequeue()
        assert(removedQuerier == querier)
        now = time.milliseconds()
        querier.close(now)
        tableQueue.enqueue(querier)

        if (results.isEmpty) {
          log.info(s"No updates for ${querier.toString}")
          return null
        }

        log.info(s"Returning ${results.size} records for ${querier.toString}")
        return results.asJava
      }
    }
    null
  }

  private def getTables(tables: List[Tuple2[String, String]])
  : List[Tuple4[String, Int, String, String]] = {
    val connection = hanaJdbcClient.getConnection
    var tableInfos: List[Tuple4[String, Int, String, String]] = List()
    val noOfTables = tables.size
    var tablecount = 1

    var stmtToFetchPartitions = s"SELECT SCHEMA_NAME, TABLE_NAME, PARTITION FROM SYS.M_CS_PARTITIONS WHERE "
    tables.foreach(table => {
      table._1 match {
        case Parameters.TABLE_NAME_FORMAT(schema, tablename) =>
          stmtToFetchPartitions += s"(SCHEMA_NAME = '$schema' AND TABLE_NAME = '$tablename')"

          if (tablecount < noOfTables) {
            stmtToFetchPartitions += " OR "
          }
          tablecount = tablecount + 1
        case _ =>
          throw new HANAClientException("The table name is invalid. Does not follow naming conventions")
      }
    })

    val stmt = connection.createStatement()
    val partitionRs = stmt.executeQuery(stmtToFetchPartitions)

    while(partitionRs.next()) {
      val tableName = "\"" + partitionRs.getString(1) + "\".\"" + partitionRs.getString(2) + "\""
      tableInfos :+= Tuple4(tableName, partitionRs.getInt(3), tableName + partitionRs.getInt(3),
        tables.filter(table => table._1 == tableName)
          .map(table => table._2).head.toString)
    }
    tableInfos
  }
}