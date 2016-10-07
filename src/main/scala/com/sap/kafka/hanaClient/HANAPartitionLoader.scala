package com.sap.kafka.hanaClient

import com.sap.kafka.connect.config.HANAConfig

/**
 * The [[AbstractHANAPartitionLoader]] which uses [[HANAJdbcClient]].
 */
object HANAPartitionLoader extends AbstractHANAPartitionLoader {

  /** @inheritdoc */
  def getHANAJdbcClient(hanaConfiguration: HANAConfig): HANAJdbcClient =
    new HANAJdbcClient(hanaConfiguration)

}
