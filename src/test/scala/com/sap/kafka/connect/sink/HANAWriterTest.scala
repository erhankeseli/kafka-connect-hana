package com.sap.kafka.connect.sink

import java.sql.SQLException
import java.util

import com.sap.kafka.connect.MockJdbcClient
import com.sap.kafka.connect.config.Parameters
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.FunSuite

class HANAWriterTest extends FunSuite {
  test("test with auto.create mode set to true") {
    val topic = "testTopic"

    val props = new util.HashMap[String, String]()
    props.put("connection.url", "jdbc:h2:mem:test;" +
      "INIT=CREATE SCHEMA IF NOT EXISTS TEST")
    props.put("connection.user", "sa")
    props.put("connection.password", "sa")
    props.put("topics", "testTopic")
    props.put("testTopic.table.name", "\"TEST\".\"BOOKS\"")
    props.put("testTopic.table.type", "row")
    props.put("auto.create", "true")

    val config = Parameters.getConfig(props)
    val mockJdbcClient = new MockJdbcClient(config)
    val writer = new HANAWriter(config, mockJdbcClient)

    val valueSchema = SchemaBuilder.struct()
                        .field("author", Schema.STRING_SCHEMA)
                        .field("title", Schema.STRING_SCHEMA)
                        .build()

    val valueStruct = new Struct(valueSchema)
                        .put("author", "Tom Robbins")
                        .put("title", "Villa Incognito")

    val record = new SinkRecord(topic, 0, null, null, valueSchema, valueStruct, 0)
    writer.write(util.Collections.singleton(record))

    val structs = mockJdbcClient.executeQuery(valueSchema, "select * from \"TEST\".\"BOOKS\"",
      -1, -1)

    assert(structs.get.size === 1)
  }

  test("test with data in both key & value") {
    val topic = "testTopic"

    val props = new util.HashMap[String, String]()
    props.put("connection.url", "jdbc:h2:mem:test;" +
      "INIT=CREATE SCHEMA IF NOT EXISTS TEST")
    props.put("connection.user", "sa")
    props.put("connection.password", "sa")
    props.put("topics", "testTopic")
    props.put("testTopic.table.name", "\"TEST\".\"BOOKS_KEY_VALUE\"")
    props.put("testTopic.table.type", "row")
    props.put("auto.create", "true")

    val config = Parameters.getConfig(props)
    val mockJdbcClient = new MockJdbcClient(config)
    val writer = new HANAWriter(config, mockJdbcClient)

    val keySchema = SchemaBuilder.struct()
                      .field("author", Schema.STRING_SCHEMA)
                      .build()

    val keyStruct = new Struct(keySchema)
                    .put("author", "Tom Robbins")

    val valueSchema = SchemaBuilder.struct()
                        .field("title", Schema.STRING_SCHEMA)
                        .build()

    val valueStruct = new Struct(valueSchema)
                        .put("title", "Villa Incognito")

    val record = new SinkRecord(topic, 0, keySchema, keyStruct, valueSchema, valueStruct, 0)
    writer.write(util.Collections.singleton(record))

    val combinedSchema = SchemaBuilder.struct()
                          .field("author", Schema.STRING_SCHEMA)
                          .field("title", Schema.STRING_SCHEMA)
                          .build()

    val structs = mockJdbcClient.executeQuery(combinedSchema, "select * from \"TEST\".\"BOOKS_KEY_VALUE\"",
      -1, -1)

    assert(structs.get.size === 1)

    val actualData = structs.get.head

    val expectedAuthor = "Tom Robbins"
    val actualAuthor = actualData.get("author")

    val expectedTitle = "Villa Incognito"
    val actualTitle = actualData.get("title")

    assert(expectedAuthor === actualAuthor)
    assert(expectedTitle === actualTitle)
  }

  test("multi insert with primary key type none doesn't fail due to " +
    "unique constraint") {
    val tablename = "\"TEST\".\"BOOKS_PASS\""
    writeSameRecordTwice("none", null, tablename)
  }

  test("multi insert with primary key type key_value fails") {
    intercept[SQLException] {
      val tablename = "\"TEST\".\"BOOKS_AS_KEY_PASS\""
      writeSameRecordTwice("record_key", List("author"), tablename)
    }
  }

  test("multi insert with primary key type record_value fails") {
    intercept[SQLException] {
      val tablename = "\"TEST\".\"BOOKS_FAIL\""
      writeSameRecordTwice("record_value", List("author"), tablename)
    }
  }

  def writeSameRecordTwice(pkMode: String,
                           pkFields: List[String], tablename: String): Unit = {
    val topic = "testTopic"
    val partition = 7
    val offset = 42

    val props = new util.HashMap[String, String]()
    props.put("connection.url", "jdbc:h2:mem:test;" +
      "INIT=CREATE SCHEMA IF NOT EXISTS TEST")
    props.put("connection.user", "sa")
    props.put("connection.password", "sa")
    props.put("topics", "testTopic")
    props.put("testTopic.table.name", tablename)
    props.put("testTopic.table.type", "row")
    props.put("auto.create", "true")

    if (pkMode == Parameters.RECORD_KEY) {
      props.put("testTopic.pk.mode", Parameters.RECORD_KEY)
    }

    if (pkMode == Parameters.RECORD_VALUE) {
      props.put("testTopic.pk.mode", Parameters.RECORD_VALUE)
    }

    if (pkFields != null && pkFields.nonEmpty) {
      props.put("testTopic.pk.fields", pkFields.mkString(","))
    }

    val config = Parameters.getConfig(props)
    val mockJdbcClient = new MockJdbcClient(config)
    val writer = new HANAWriter(config, mockJdbcClient)

    var record: SinkRecord = null
    var results: Option[List[Struct]] = None

    if (pkMode == Parameters.RECORD_KEY) {
      val keySchema = SchemaBuilder.struct()
        .field("author", Schema.STRING_SCHEMA)
        .field("title", Schema.STRING_SCHEMA)

      val keyStruct = new Struct(keySchema)
        .put("author", "Tom Robbins")
        .put("title", "Villa Incognito")

      record = new SinkRecord(topic, partition, keySchema, keyStruct, null, null, offset)

      writer.write(util.Collections.nCopies(2, record))

      results = mockJdbcClient.executeQuery(keySchema, s"select * from $tablename",
        -1, -1)

    } else {
      val valueSchema = SchemaBuilder.struct()
        .field("author", Schema.STRING_SCHEMA)
        .field("title", Schema.STRING_SCHEMA)
        .build()

      val valueStruct = new Struct(valueSchema)
        .put("author", "Tom Robbins")
        .put("title", "Villa Incognito")

      record = new SinkRecord(topic, partition, null, null, valueSchema, valueStruct, offset)

      writer.write(util.Collections.nCopies(2, record))

      results = mockJdbcClient.executeQuery(valueSchema, s"select * from $tablename",
        -1, -1)
    }

    val structs = results.get
    assert(structs.size === 2)
  }
}