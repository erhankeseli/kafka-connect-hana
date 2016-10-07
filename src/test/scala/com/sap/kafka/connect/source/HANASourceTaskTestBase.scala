package com.sap.kafka.connect.source

import java.util

import com.sap.kafka.hanaClient.MetaSchema
import com.sap.kafka.connect.MockJdbcClient
import com.sap.kafka.connect.config.Parameters
import org.apache.kafka.common.utils.Time
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.mockito.Mockito.mock
import org.mockito.Mockito._
import org.apache.kafka.connect.data.{Field, Schema}
import org.mockito.Matchers.any

class HANASourceTaskTestBase extends FunSuite
                              with BeforeAndAfterAll {
  protected val SINGLE_TABLE_NAME = "\"TEST\".\"EMPLOYEES_SOURCE\""
  protected val SECOND_TABLE_NAME = "\"TEST\".\"EMPLOYEES_SOURCE_SECOND\""
  protected val SINGLE_TABLE_PARTITION = new util.HashMap[String, Object]()
  SINGLE_TABLE_PARTITION.put(HANASourceConnectorConstants.TABLE_NAME_KEY, SINGLE_TABLE_NAME + "_0")
  protected val SECOND_TABLE_PARTITION = new util.HashMap[String, Object]()
  SECOND_TABLE_PARTITION.put(HANASourceConnectorConstants.TABLE_NAME_KEY, SECOND_TABLE_NAME + "_0")

  protected val TOPIC = "test-topic"
  protected val SECOND_TOPIC = "test-second-topic"
  protected var time: Time = _
  protected var taskContext: SourceTaskContext = _
  protected var task: HANASourceTask = _
  protected var jdbcClient: MockJdbcClient = _

  override def beforeAll(): Unit = {
    time = new MockTime()
    jdbcClient = new MockJdbcClient(
      Parameters.getConfig(singleTableConfig()))
    task = new HANASourceTask(time, jdbcClient)

    val offsetStorageReader = mock(classOf[OffsetStorageReader])

    val partitions = new util.HashMap[String, String]()
    val tableName = SINGLE_TABLE_NAME + "0"
    partitions.put(HANASourceConnectorConstants.TABLE_NAME_KEY, tableName)
    val secondTableName = SECOND_TABLE_NAME + "0"
    partitions.put(HANASourceConnectorConstants.TABLE_NAME_KEY, secondTableName)
    val offsets: java.util.Map[java.util.Map[String, String],
      java.util.Map[String, Object]] = new util.HashMap[java.util.Map[String, String],
                                        java.util.Map[String, Object]]()
    offsets.put(partitions, null)
    when(offsetStorageReader.offsets[String](
      any(classOf[util.Collection[util.Map[String, String]]]))).thenReturn(offsets)

    taskContext = mock(classOf[SourceTaskContext])
    when(taskContext.offsetStorageReader()).thenReturn(offsetStorageReader)

    val fields = Seq(new Field("SCHEMA_NAME", 1, Schema.STRING_SCHEMA),
                     new Field("TABLE_NAME", 2, Schema.STRING_SCHEMA),
                     new Field("PARTITION", 3, Schema.INT32_SCHEMA))
    val connection = jdbcClient.getConnection
    val stmt = connection.createStatement()
    stmt.execute("CREATE SCHEMA IF NOT EXISTS SYS")

    jdbcClient.createTable(Some("SYS"), "M_CS_PARTITIONS", MetaSchema(null, fields), 3000)
    stmt.execute("insert into \"SYS\".\"M_CS_PARTITIONS\" values('TEST', 'EMPLOYEES_SOURCE', 0)")
  }

  override def afterAll(): Unit = {
    val connection = jdbcClient.getConnection
    val stmt = connection.createStatement()
    stmt.execute("drop table \"SYS\".\"M_CS_PARTITIONS\"")
  }

  protected def singleTableConfig(): java.util.Map[String, String] = {
    val props = new util.HashMap[String, String]()
    props.put("connection.url", "jdbc:h2:mem:test;" +
      "INIT=CREATE SCHEMA IF NOT EXISTS TEST")
    props.put("connection.user", "sa")
    props.put("connection.password", "sa")
    props.put("mode", "bulk")
    props.put("topics", TOPIC)
    props.put(s"$TOPIC.table.name", SINGLE_TABLE_NAME)
    props.put(s"$TOPIC.partition.count", "1")
    props.put(s"$TOPIC.poll.interval.ms", "60000")

    props
  }

  protected def multiTableConfig(): java.util.Map[String, String] = {
    val props = new util.HashMap[String, String]()
    props.put("connection.url", "jdbc:h2:mem:test;" +
      "INIT=CREATE SCHEMA IF NOT EXISTS TEST")
    props.put("connection.user", "sa")
    props.put("connection.password", "sa")
    props.put("mode", "bulk")
    props.put("topics", s"$TOPIC,$SECOND_TOPIC")

    props.put(s"$TOPIC.table.name", SINGLE_TABLE_NAME)
    props.put(s"$TOPIC.partition.count", "1")
    props.put(s"$TOPIC.poll.interval.ms", "60000")

    props.put(s"$SECOND_TOPIC.table.name", SECOND_TABLE_NAME)
    props.put(s"$SECOND_TOPIC.partition.count", "1")
    props.put(s"$SECOND_TOPIC.poll.interval.ms", "60000")

    props
  }

  protected def expectInitialize(partitions: util.Collection[java.util.Map[String, Object]],
                                 offsets: java.util.Map[java.util.Map[String, Object],
                                   java.util.Map[String, Object]]): Unit = {
    val reader = mock(classOf[OffsetStorageReader])
    when(taskContext.offsetStorageReader()).thenReturn(reader)
    when(reader.offsets(partitions)).thenReturn(offsets)
  }

  protected def expectInitializeNoOffsets(partitions: util.Collection[util.Map[String, Object]])
  : Unit = {
    val offsets = new util.HashMap[java.util.Map[String, Object],
      java.util.Map[String, Object]]()

    val iterator = partitions.iterator()
    while (iterator.hasNext) {
      val partition = iterator.next()
      offsets.put(partition, null)
    }

    expectInitialize(partitions, offsets)
  }

  protected def initializeTask(): Unit = {
    task.initialize(taskContext)
  }
}