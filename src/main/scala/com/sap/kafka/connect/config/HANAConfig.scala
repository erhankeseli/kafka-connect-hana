package com.sap.kafka.connect.config

import scala.collection.JavaConversions._

object Parameters {
  val RECORD_KEY = "record_key"
  val RECORD_VALUE = "record_value"

  val TABLE_NAME_FORMAT = "\"(.+)\"\\.\"(.+)\"".r

  val MODE_BULK = "bulk"
  val MODE_INCREMENTING = "incrementing"

  val COLUMN_TABLE_TYPE = "column"
  val ROW_TABLE_TYPE = "row"

  val NO_PARTITION = "none"
  val HASH_PARTITION = "hash"
  val ROUND_ROBIN_PARTITION = "round_robin"

  def getConfig(props: java.util.Map[String, String]): HANAConfig = {
    if (props.get("connection.url") == null) {
      throw new IllegalArgumentException("Mandatory parameter missing: " +
        " HANA DB Jdbc url must be specified in 'connection.url' parameter")
    }

    if (props.get("connection.user") == null) {
      throw new IllegalArgumentException("Mandatory parameter missing: " +
        " HANA DB user must be specified in 'connection.user' parameter")
    }

    if (props.get("connection.password") == null) {
      throw new IllegalArgumentException("Mandatory parameter missing: " +
        " HANA DB password must be specified in 'connection.password' parameter")
    }

    if (props.get("topics") == null) {
      throw new IllegalArgumentException("Mandatory parameter missing: " +
        "A comma-separated list of topics is required to run the HANA-Kafka connectors")
    }

    HANAConfig(props.toMap)
  }
}

case class HANAConfig(props: Map[String, String]) {
  /**
    * HANA DB Jdbc url for source & sink
    */
  def connectionUrl = props("connection.url")

  /**
    * HANA DB Jdbc user for source & sink
    */
  def connectionUser = props("connection.user")

  /**
    * HANA DB Jdbc password for source & sink
    */
  def connectionPassword = props("connection.password")

  /**
    * List of topics to be published or subscribed by source or sink
    */
  def topics = props("topics").toString.split(",").toList

  /**
    * Batch size for sink. Should be an integer.
    * Default is 3000.
    */
  def batchSize = props.getOrElse[String]("batch.size", "3000").toInt

  /**
    * Max retries for sink. Should be an integer.
    * Default is 10.
    */
  def maxRetries = props.getOrElse[String]("max.retries", "10").toInt

  /**
    * Auto create HANA tables if table is not found for Sink.
    * Default is false.
    */
  def autoCreate = props.getOrElse[String]("auto.create", "false").toBoolean

  /**
    * Max rows to include in a single batch call for source. Should be an integer.
    * Default is 100.
    */
  def batchMaxRows = props.getOrElse[String]("batch.max.rows", "100").toInt

  /**
    * The mode for updating a table each time it is polled.
    * Default is 'bulk'.
    */
  def mode = props.getOrElse[String]("mode", Parameters.MODE_BULK)

  def topicProperties(topic: String) = {
    val topicPropMap = scala.collection.mutable.Map[String, String]()

    for ((key, value) <- props) {
      /**
        * table name to fetch from or write to by source & sink
        */
      if (key == s"$topic.table.name") {
        topicPropMap.put("table.name", value)
      }

      /**
        * primary key mode to be used by sink.
        * Default is none.
        */
      if (key == s"$topic.pk.mode") {
        topicPropMap.put("pk.mode", value)
      }

      /**
        * comma-separated primary key fields to be used by sink
        */
      if (key == s"$topic.pk.fields") {
        topicPropMap.put("pk.fields", value)
      }

      /**
        * poll interval time to be used by source
        * Default value is 60000
        */
      if (key == s"$topic.poll.interval.ms") {
        topicPropMap.put("poll.interval.ms", value)
      }

      /**
        * incrementing column name to be used by source
        */
      if (key == s"$topic.incrementing.column.name") {
        topicPropMap.put("incrementing.column.name", value)
      }

      /**
        * topic partition count to be used by source.
        * Default value is 1.
        */
      if (key == s"$topic.partition.count") {
        topicPropMap.put("partition.count", value)
      }

      /**
        * table partition mode to be used by sink
        * Default value is none.
        */
      if (key == s"$topic.table.partition.mode") {
        if (value == Parameters.ROUND_ROBIN_PARTITION)
          topicPropMap.put("table.partition.mode", value)
        else if (value == Parameters.HASH_PARTITION)
          topicPropMap.put("table.partition.mode", value)
      }

      /**
        * table partition count to be used by sink
        * Default value is 0.
        */
      if (key == s"$topic.table.partition.count") {
        topicPropMap.put("table.partition.count", value)
      }

      /**
        * table type to be used by sink
        * Default value is COLUMN_TABLE_TYPE.
        */
      if (key == s"$topic.table.type") {
        if (value == Parameters.COLUMN_TABLE_TYPE)
          topicPropMap.put("table.type", value)
        else if (value == Parameters.ROW_TABLE_TYPE)
          topicPropMap.put("table.type", value)
        else
          throw new IllegalArgumentException(
            "Value specified is incorrect for 'table.type' parameter")
      }
    }

    if (topicPropMap.get("table.name").isEmpty) {
      throw new IllegalArgumentException("A table name must be specified for HANA-Kafka " +
        "connectors to work")
    }

    if (topicPropMap.get("incrementing.column.name").isEmpty &&
      (mode == Parameters.MODE_INCREMENTING)) {
      throw new IllegalArgumentException(s"With mode as ${Parameters.MODE_INCREMENTING}" +
        s" an incrementing column must be specified")
    }

    if (topicPropMap.get("pk.mode").isEmpty) {
      topicPropMap.put("pk.mode", "none")
    }

    if (topicPropMap.get("pk.fields").isEmpty) {
      topicPropMap.put("pk.fields", "")
    }

    if (topicPropMap.get("poll.interval.ms").isEmpty) {
      topicPropMap.put("poll.interval.ms", "60000")
    }

    if (topicPropMap.get("table.type").isEmpty) {
      topicPropMap.put("table.type", Parameters.COLUMN_TABLE_TYPE)
    }

    if (topicPropMap.get("table.partition.mode").isEmpty) {
      topicPropMap.put("table.partition.mode", Parameters.NO_PARTITION)
    }

    if (topicPropMap.get("table.partition.count").isEmpty) {
      topicPropMap.put("table.partition.count", "0")
    }

    if (topicPropMap.get("partition.count").isEmpty) {
      topicPropMap.put("partition.count", "1")
    }

    topicPropMap.toMap
  }
}