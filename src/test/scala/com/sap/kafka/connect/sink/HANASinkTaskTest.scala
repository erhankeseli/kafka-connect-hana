package com.sap.kafka.connect.sink

import java.util

import com.sap.kafka.connect.MockJdbcClient
import com.sap.kafka.connect.config.Parameters
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.scalatest.FunSuite
import org.mockito.Mockito.mock

class HANASinkTaskTest extends FunSuite {

  test("put propagates to db with auto.create mode to true " +
    "and pk.mode to record_value") {
    val schema = SchemaBuilder.struct().name("employee")
      .field("employee_id", Schema.INT32_SCHEMA)
      .field("employee_name", Schema.STRING_SCHEMA)
      .build()

    val props = new util.HashMap[String, String]()
    props.put("connection.url", "jdbc:h2:mem:test;" +
      "INIT=CREATE SCHEMA IF NOT EXISTS TEST")
    props.put("connection.user", "sa")
    props.put("connection.password", "sa")
    props.put("topics", "testTopic")
    props.put("testTopic.table.name", "\"TEST\".\"EMPLOYEES\"")
    props.put("testTopic.table.type", "row")
    props.put("auto.create", "true")
    props.put("testTopic.pk.mode", "record_value")
    props.put("testTopic.pk.fields", "employee_id")

    val task = new HANASinkTask()

    task.initialize(mock(classOf[SinkTaskContext]))

    task.start(props)

    val config = Parameters.getConfig(props)
    task.hanaClient = new MockJdbcClient(config)
    task.initWriter(config)

    val struct = new Struct(schema).put("employee_id", 1)
      .put("employee_name", "Lukas")

    task.put(util.Collections.singleton(
      new SinkRecord("testTopic", 1, null, null, schema, struct, 42)
    ))

    val rs = task.hanaClient.executeQuery(schema, "select * from \"TEST\".\"EMPLOYEES\"",
      -1, -1)

    val structs = rs.get
    assert(structs.size == 1)
    val head = structs.head

    val expectedEmployeeId = struct.get("employee_id")
    val actualEmployeeId = head.getInt32("employee_id")
    assert(expectedEmployeeId === actualEmployeeId)

    val expectedEmployeeName = struct.get("employee_name")
    val actualEmployeeName = head.getString("employee_name")
    assert(expectedEmployeeName === actualEmployeeName)
  }

  test("put propagates to db with auto.create mode to true " +
    "and pk.mode to none") {
    val schema = SchemaBuilder.struct().name("employee_without_keys")
                      .field("employee_id", Schema.INT32_SCHEMA)
                      .field("employee_name", Schema.STRING_SCHEMA)
                      .build()

    val props = new util.HashMap[String, String]()
    props.put("connection.url", "jdbc:h2:mem:test;" +
      "INIT=CREATE SCHEMA IF NOT EXISTS TEST")
    props.put("connection.user", "sa")
    props.put("connection.password", "sa")
    props.put("topics", "testTopic")
    props.put("testTopic.table.name", "\"TEST\".\"EMPLOYEES_WITHOUT_KEYS\"")
    props.put("testTopic.table.type", "row")
    props.put("auto.create", "true")

    val task = new HANASinkTask()

    task.initialize(mock(classOf[SinkTaskContext]))

    task.start(props)

    val config = Parameters.getConfig(props)
    task.hanaClient = new MockJdbcClient(config)
    task.initWriter(config)

    val struct = new Struct(schema).put("employee_id", 1)
                    .put("employee_name", "Lukas")

    task.put(util.Collections.singleton(
      new SinkRecord("testTopic", 1, null, null, schema, struct, 42)
    ))

    val rs = task.hanaClient.executeQuery(schema, "select * from \"TEST\".\"EMPLOYEES_WITHOUT_KEYS\"",
                                        -1, -1)

    val structs = rs.get
    assert(structs.size === 1)
    val head = structs.head

    val expectedEmployeeId = struct.get("employee_id")
    val actualEmployeeId = head.getInt32("employee_id")
    assert(expectedEmployeeId === actualEmployeeId)

    val expectedEmployeeName = struct.get("employee_name")
    val actualEmployeeName = head.getString("employee_name")
    assert(expectedEmployeeName === actualEmployeeName)
  }

  test("put propagates to db with auto.create mode to true " +
    "and pk.mode to record_value for multiple topics") {
    val schema_table1 = SchemaBuilder.struct().name("table1")
                          .field("id", Schema.INT32_SCHEMA)
                          .field("name", Schema.STRING_SCHEMA)
                          .build()

    val schema_table2 = SchemaBuilder.struct().name("table2")
                          .field("id", Schema.INT32_SCHEMA)
                          .field("name", Schema.STRING_SCHEMA)
                          .build()

    val props = new util.HashMap[String, String]()
    props.put("connection.url", "jdbc:h2:mem:test;" +
      "INIT=CREATE SCHEMA IF NOT EXISTS TEST")
    props.put("connection.user", "sa")
    props.put("connection.password", "sa")
    props.put("topics", "testTopic1,testTopic2")
    props.put("auto.create", "true")

    props.put("testTopic1.table.name", "\"TEST\".\"TABLE1\"")
    props.put("testTopic1.table.type", "row")

    props.put("testTopic2.table.name", "\"TEST\".\"TABLE2\"")
    props.put("testTopic2.table.type", "row")

    val task = new HANASinkTask()

    task.initialize(mock(classOf[SinkTaskContext]))

    task.start(props)

    val config = Parameters.getConfig(props)
    task.hanaClient = new MockJdbcClient(config)
    task.initWriter(config)

    val struct_table1 = new Struct(schema_table1)
                        .put("id", 1)
                        .put("name", "Lukas")

    val struct_table2 = new Struct(schema_table2)
                        .put("id", 1)
                        .put("name", "Ted")

    val sinkRecords = new util.ArrayList[SinkRecord]()
    sinkRecords.add(new SinkRecord("testTopic1", 1, null, null,
      schema_table1, struct_table1, 42))
    sinkRecords.add(new SinkRecord("testTopic2", 1, null, null,
      schema_table2, struct_table2, 42))

    task.put(sinkRecords)

    val rs1 = task.hanaClient.executeQuery(schema_table1, "select * from \"TEST\".\"TABLE1\"",
                                          -1, -1)

    assert(rs1.get.size === 1)
    val table1_record = rs1.get.head

    val expectedId1 = 1
    val actualId1 = table1_record.getInt32("id")

    val expectedName1 = "Lukas"
    val actualName1 = table1_record.getString("name")

    assert(expectedId1 === actualId1)
    assert(expectedName1 === actualName1)

    val rs2 = task.hanaClient.executeQuery(schema_table2, "select * from \"TEST\".\"TABLE2\"",
                                          -1, -1)

    assert(rs2.get.size === 1)
    val table_record2 = rs2.get.head

    val expectedId2 = 1
    val actualId2 = table_record2.getInt32("id")

    val expectedName2 = "Ted"
    val actualName2 = table_record2.getString("name")

    assert(expectedId2 === actualId2)
    assert(expectedName2 === actualName2)
  }
}