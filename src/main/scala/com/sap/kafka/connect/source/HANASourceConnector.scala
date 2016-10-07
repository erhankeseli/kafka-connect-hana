package com.sap.kafka.connect.source

import java.util

import com.sap.kafka.connect.config.HANAConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector
import com.sap.kafka.connect.config.Parameters

import scala.collection.JavaConverters._

class HANASourceConnector extends SourceConnector {
  private var configProperties: Option[util.Map[String, String]] = None
  private var sourceconfig: Option[HANAConfig] = None

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def start(properties: util.Map[String, String]): Unit = {
    try {
      configProperties = Some(properties)
      sourceconfig = Some(Parameters.getConfig(configProperties.get))
    } catch {
      case e: Exception => throw new ConnectException("Couldn't start JdbcSourceConnector " +
        "due to configuration error", e)
    }
  }

  override def taskClass(): Class[_ <: Task] = classOf[HANASourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    (1 to maxTasks).map(c => configProperties.get).toList.asJava
  }

  override def stop(): Unit = {

  }

  override def config(): ConfigDef = {
    new ConfigDef
  }
}