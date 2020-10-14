package com.ringcentral.analytics.etl.config

import java.sql.Types

import com.ringcentral.analytics.etl.config.model.DataTypeDefinition
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object DataTypeParser {

    val LOG: Logger = LoggerFactory.getLogger(DataTypeParser.getClass)

    def dataTypeToSqlString(dataType: DataTypeDefinition): String = {
        val precisionScalePart: String = (dataType.precision ++ dataType.scale).toSeq.mkString(",")
        val sizePart: String = dataType.size.map(_.toString).getOrElse("")
        val extraPart = precisionScalePart ++ sizePart
        if (extraPart.nonEmpty) s"${dataType.name}($extraPart)" else dataType.name
    }

    def dataTypeFromString(s: String): DataTypeDefinition = {
        val maybeTypeName = parseTypeName(s)
        val (precision, scale) = parsePrecisionAndScale(s)
        DataTypeDefinition(
            maybeTypeName.getOrElse(throw new RuntimeException(s"Unable to parse sql type $s")),
            hiveDataType(maybeTypeName.get.toUpperCase, precision, scale),
            jdbcDataType(maybeTypeName.get),
            precision,
            scale,
            parseSize(s)
        )
    }

    private[config] def parseTypeName(s: String): Option[String] = {
        val dataTypePattern = "([\\sA-Za-z]+)".r
        dataTypePattern.findFirstMatchIn(s).map(_.group(1)).map(_.toUpperCase).map(_.trim)
    }

    private[config] def parseSize(s: String): Option[Int] = {
        val sizePattern = "\\(\\s*(\\d+)\\s*\\)".r
        sizePattern.findFirstMatchIn(s).map(_.group(1).toInt)
    }

    private[config] def parsePrecisionAndScale(s: String): (Option[Int], Option[Int]) = {
        val precisionScalePattern = "\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)".r

        val precision = precisionScalePattern.findFirstMatchIn(s).map(s => s.group(1).toInt)
        val scale = precisionScalePattern.findFirstMatchIn(s).map(s => s.group(2).toInt)
        (precision, scale)
    }

    private[config] def hiveDataType(dataType: String, precision: Option[Int], scale: Option[Int]): DataType = {
        dataType.toUpperCase match {
            case "BIGINT" => DataTypes.LongType
            case "BINARY" | "VARBINARY" | "LONG VARBINARY" | "LONGVARBINARY" => DataTypes.BinaryType
            case "BIT" => DataTypes.BooleanType
            case "CHAR" | "VARCHAR" | "LONG VARCHAR" | "LONGVARCHAR" => DataTypes.StringType
            case "DATE" => DataTypes.DateType
            case "DECIMAL" | "NUMERIC" => createDecimalType(precision, scale)
            case "DOUBLE" | "FLOAT" | "REAL" => DataTypes.DoubleType
            case "INTEGER" | "INT" => DataTypes.IntegerType
            case "SMALLINT" => DataTypes.ShortType
            case "TIME" | "TIMESTAMP" => DataTypes.TimestampType
            case "TINYINT" => DataTypes.ByteType
            case "BOOLEAN" => DataTypes.BooleanType
            case _ => throw new MisconfigurationException(s"Not supported data type $dataType")
        }
    }

    private def createDecimalType(precision: Option[Int], scale: Option[Int]) = {
        if (precision.isEmpty || scale.isEmpty) {
            LOG.warn("Precision or scale is not specified, falling back to default values of precision and scale")
            DataTypes.createDecimalType()
        } else {
            DataTypes.createDecimalType(precision.get, scale.get)
        }
    }

    private[config] def jdbcDataType(dataType: String): Int = {
        dataType.toUpperCase match {
            case "BIGINT" => Types.BIGINT
            case "BINARY" | "LONG VARBINARY" | "LONGVARBINARY" => Types.BINARY
            case "BIT" => Types.BOOLEAN
            case "CHAR" => Types.CHAR
            case "DATE" => Types.DATE
            case "DECIMAL" => Types.DECIMAL
            case "DOUBLE" | "FLOAT" | "REAL" => Types.DOUBLE
            case "INTEGER" | "INT" => Types.INTEGER
            case "LONG VARCHAR" | "LONGVARCHAR" => Types.LONGVARCHAR
            case "NUMERIC" => Types.NUMERIC
            case "SMALLINT" => Types.SMALLINT
            case "TIME" => Types.TIME
            case "TIMESTAMP" => Types.TIMESTAMP
            case "TINYINT" => Types.TINYINT
            case "VARBINARY" => Types.VARBINARY
            case "VARCHAR" => Types.VARCHAR
            case "BOOLEAN" => Types.BOOLEAN
            case _ => throw new MisconfigurationException(s"Not supported data type $dataType")
        }
    }
}
