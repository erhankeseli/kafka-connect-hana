package com.sap.kafka.utils

import com.sap.kafka.hanaClient.MetaSchema
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Schema.Type


object SchemaBuilder {
  /**
   * Converts a AVRO schema to the HANA Schema.
   *
   * @param schema The schema to convert
   * @return The HANA schema as [[String]]
   */
  def avroToHANASchema(schema: MetaSchema): String =
    schema.avroFields.map({ field =>
      val name = field.name
      val hanaType = avroToHANAType(field.schema().`type`())
      val nullModifier = if (field.schema().isOptional) "NULL" else "NOT NULL"
      s""""$name" $hanaType $nullModifier"""
    }).mkString(", ")

  /**
   * Transforms a Spark type to the corresponding HANA type.
   *
   * @param avroType Avro type
   * @return HANA type
   */
  def avroToHANAType(avroType: Type): String = {
    typeToSql(avroType)
  }

  // scalastyle:off cyclomatic.complexity
  def typeToSql(schemaType: Type): String =
    schemaType match {
      case Schema.Type.INT8 => "TINYINT"
      case Schema.Type.INT16 => "SMALLINT"
      case Schema.Type.INT32 => "INTEGER"
      case Schema.Type.INT64 => "BIGINT"
      case Schema.Type.FLOAT32 => "FLOAT"
      case Schema.Type.FLOAT64 => "DOUBLE"
      case Schema.Type.BOOLEAN => "BOOLEAN"
      case Schema.Type.STRING => "VARCHAR(*)"
      case _ =>
        throw new IllegalArgumentException(s"Type $schemaType cannot be converted to SQL type")
    }
  // scalastyle:on cyclomatic.complexity
}
