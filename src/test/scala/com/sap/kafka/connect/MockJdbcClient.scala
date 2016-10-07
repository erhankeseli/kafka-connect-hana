package com.sap.kafka.connect

import com.sap.kafka.hanaClient.HANAJdbcClient
import com.sap.kafka.connect.config.HANAConfig

class MockJdbcClient(configuration: HANAConfig)
  extends HANAJdbcClient(configuration) {
  override val driver: String = "org.h2.Driver"
}