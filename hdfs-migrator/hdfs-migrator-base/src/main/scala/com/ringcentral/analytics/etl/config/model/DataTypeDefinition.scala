package com.ringcentral.analytics.etl.config.model

import org.apache.spark.sql.types.DataType

/**
 * Definition of the data type used by JDBC-hadoop library. Contains information about SQL type, Spark/Hive type, Destination database type
 *
 * @param name       data type name, see e.g. https://www.cis.upenn.edu/~bcpierce/courses/629/jdkdocs/guide/jdbc/getstart/mapping.doc.html
 * @param sparkType  SparkSQL data type (org.apache.spark.sql.types.DataType) - will be used to store data in Hadoop
 * @param jdbcTypeId java.sql.Types constant for this data type
 * @param precision  Precision for numeric types
 * @param scale      Scale for numeric types
 * @param size       Size (in bytes) for character types
 */
case class DataTypeDefinition(
                                 name: String,
                                 sparkType: DataType,
                                 jdbcTypeId: Int,
                                 precision: Option[Int],
                                 scale: Option[Int],
                                 size: Option[Int]
                             )
