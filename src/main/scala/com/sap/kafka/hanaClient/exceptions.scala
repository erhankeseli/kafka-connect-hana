package com.sap.kafka.hanaClient

import com.sap.kafka.connect.config.HANAConfig

class HANAClientException(msg: String, cause: Throwable*)
  extends RuntimeException(msg)

class HANAJdbcException(config: HANAConfig, msg: String, cause: Throwable)
  extends HANAClientException(s"[$config] $msg", cause)

class HANAJdbcBadStateException(config: HANAConfig, msg: String, cause: Throwable)
  extends HANAJdbcException(config, msg, cause)

class HANAJdbcConnectionException(config: HANAConfig, msg: String, cause: Throwable)
  extends HANAJdbcException(config, msg, cause)
