package com.sap.kafka.connect.sink

import java.sql.{Connection, SQLException}

import com.sap.kafka.hanaClient.{HANAClientException, HANAJdbcClient, MetaSchema, metaAttr}
import com.sap.kafka.connect.config.{HANAConfig, Parameters}
import com.sap.kafka.schema.KeyValueSchema
import com.sap.kafka.utils.JdbcTypeConverter
import org.apache.kafka.connect.data.{Field, Schema}
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._


class HANASinkRecordsCollector(var tableName: String, client: HANAJdbcClient,
                               connection: Connection, config: HANAConfig) {
  private val log: Logger = LoggerFactory.getLogger(classOf[HANASinkTask])
  private var records: Seq[SinkRecord] = Seq[SinkRecord]()
  private var tableMetaData:Seq[metaAttr] = Seq[metaAttr]()
  private var metaSchema: MetaSchema = null
  var tableConfigInitialized = false

  private def initTableConfig(nameSpace: Option[String], tableName: String) : Boolean = {

    tableConfigInitialized match {
      case false =>
        if(client.tableExists(nameSpace, tableName)){
          tableMetaData = client.getMetaData(tableName, nameSpace)
          metaSchema = new MetaSchema(tableMetaData, null)
          tableConfigInitialized = true
        }
      case true =>
    }
    tableConfigInitialized
  }

  @throws (classOf[SQLException])
  private[sink] def add(record: SinkRecord): Seq[SinkRecord] = {
    val recordSchema = KeyValueSchema(record.keySchema(),record.valueSchema())
    var flushedRecords = Seq[SinkRecord]()

    initTableConfig(getTableName._1,getTableName._2) match
    {
      case true =>
        log.info(s"""Table $tableName exists.Validate the schema and check if schema needs to evolve""")
        var recordFields = Seq[metaAttr]()

        if (recordSchema.keySchema != null) {
          for (field <- recordSchema.keySchema.fields) {
            val fieldSchema: Schema = field.schema()
            val fieldAttr = metaAttr(field.name(),
              JdbcTypeConverter.convertToHANAType(fieldSchema.`type`()), 1, 0, 0, isSigned = false)
            recordFields = recordFields :+ fieldAttr
          }
        }

        if (recordSchema.valueSchema != null) {
          for (field <- recordSchema.valueSchema.fields) {
            val fieldSchema: Schema = field.schema
            val fieldAttr = metaAttr(field.name(),
              JdbcTypeConverter.convertToHANAType(fieldSchema.`type`()), 1, 0, 0, isSigned = false)
            recordFields = recordFields :+ fieldAttr
          }
        }
        if(!compareSchema(recordFields))
          {
            log.error(
              s"""Table $tableName has a different schema from the record Schema.
                 |Auto Evolution of schema is not supported""".stripMargin)
            throw new ConnectException(
              s"""Table $tableName has a different schema from the Record Schema.
                 |Auto Evolution of schema is not supported
               """.stripMargin)
          }
      case false =>
        if (config.autoCreate) {
          // find table type
          val tableType = if (config.topicProperties(record.topic())
            .get("table.type").get == "column")
            true
          else false

          // find partition type
          val partitionType = config.topicProperties(record.topic())
            .get("table.partition.mode").get

          //find no. of partitions
          val partitionCount = config.topicProperties(record.topic())
            .get("table.partition.count").get

          metaSchema = new MetaSchema(Seq[metaAttr](), Seq[Field]())

          if (recordSchema.keySchema != null) {
            for (field <- recordSchema.keySchema.fields) {
              val fieldSchema: Schema = field.schema
              val fieldAttr = metaAttr(field.name(),
                JdbcTypeConverter.convertToHANAType(fieldSchema.`type`()), 1, 0, 0, isSigned = false)
              metaSchema.fields = metaSchema.fields :+ fieldAttr
              metaSchema.avroFields = metaSchema.avroFields :+ field
            }
          }

          if (recordSchema.valueSchema != null) {
            for (field <- recordSchema.valueSchema.fields) {
              val fieldSchema: Schema = field.schema
              val fieldAttr = metaAttr(field.name(),
                JdbcTypeConverter.convertToHANAType(fieldSchema.`type`()), 1, 0, 0, isSigned = false)
              metaSchema.fields = metaSchema.fields :+ fieldAttr
              metaSchema.avroFields = metaSchema.avroFields :+ field
            }
          }

          if (config.topicProperties(record.topic()).get("pk.mode").get
            == Parameters.RECORD_KEY) {
            val keys = getValidKeys(config.topicProperties(record.topic())
              .get("pk.fields").get.split(",").toList, metaSchema.avroFields)

            try {
              client.createTable(getTableName._1, getTableName._2, metaSchema,
                config.batchSize, tableType, keys, partitionType, partitionCount.toInt)
            }
          }
          else if (config.topicProperties(record.topic()).get("pk.mode").get
            == Parameters.RECORD_VALUE) {
            val keys = getValidKeys(config.topicProperties(record.topic())
              .get("pk.fields").get.split(",").toList, metaSchema.avroFields)
            try{
              client.createTable(getTableName._1, getTableName._2, metaSchema,
              config.batchSize, tableType, keys, partitionType, partitionCount.toInt)

            }
          } else {
            try {
              client.createTable(getTableName._1, getTableName._2,
                metaSchema, config.batchSize, tableType, partitionType = partitionType,
                partitionCount = partitionCount.toInt)
            }
          }
          client.getMetaData(getTableName._2,getTableName._1)
        } else {
          throw new ConnectException(s"Table does not exist. Set 'auto.create' parameter to true")
        }
    }
    if (records.size == config.batchSize) {
      flushedRecords = flush()
      records = records :+ record
    }
    else{
      records = records :+ record
      flushedRecords = Seq.empty[SinkRecord]
    }
    flushedRecords
  }

  @throws (classOf[SQLException])
  private[sink] def flush(): Seq[SinkRecord] = {
    client.loadData(getTableName._1, getTableName._2, connection, metaSchema, records,  config.batchSize)
    val flushedRecords = records
    records = Seq.empty[SinkRecord]
    flushedRecords
  }

  private[sink] def size(): Int = {
    records.size
  }

  private def getTableName: (Option[String], String) = {
    tableName match {
      case Parameters.TABLE_NAME_FORMAT(schema, table) =>
        (Some(schema), table)
      case _ =>
        throw new HANAClientException("The table name is invalid. Does not follow naming conventions")
    }
  }

  private def getValidKeys(keys: List[String], allFields: Seq[Field]): List[String] = {
    val fields = allFields.map(metaAttr => metaAttr.name())
    keys.filter(key => fields.contains(key))
  }

  private def compareSchema(dbSchema : Seq[metaAttr]): Boolean = {
    val fieldNames = metaSchema.fields.map(_.name)
    if(metaSchema.fields.size != dbSchema.size)
      false
    else
      {
        for (field <- dbSchema) {
          if (!fieldNames.contains(field.name)){
           return false
          }
        }
      true
      }
  }

}
