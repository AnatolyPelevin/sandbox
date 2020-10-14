package com.ringcentral.analytics.etl.config

import com.ringcentral.analytics.etl.common.JsonParser
import com.ringcentral.analytics.etl.config.model.FieldDefinition
import com.ringcentral.analytics.etl.config.model.TableDefinition
import scalikejdbc.DBSession
import scalikejdbc.WrappedResultSet
import scalikejdbc.interpolation.Implicits.scalikejdbcSQLInterpolationImplicitDef
import scalikejdbc.interpolation.SQLSyntax

class FieldConfigReader(options: EtlDbConnectionOptions, fieldsConfigTableName: String) {

    def addToTableDefinition(tableDefinition: TableDefinition)(implicit session: DBSession): TableDefinition = {
        val fieldDefinitions: Seq[FieldDefinition] = readFieldDefinitionsFromDb(tableDefinition)
        addSorted(tableDefinition, fieldDefinitions)
    }

    private[config] def readFieldDefinitionsFromDb(tc: TableDefinition)(implicit session: DBSession): Seq[FieldDefinition] = {
        fieldDefinitionsSelect(tc.hiveTableName, tc.version).map(extractFieldDefinition).list().apply()
    }

    private[config] def addSorted(tableDefinition: TableDefinition, fieldDefinitions: Seq[FieldDefinition]): TableDefinition = {
        if (fieldDefinitions.isEmpty) {
            throw new MisconfigurationException(s"Table definition $tableDefinition doesn't have any associated field definitions")
        }
        val toAdd = fieldDefinitions.sortWith(_.order < _.order)

        tableDefinition.fieldDefinitions = toAdd
        tableDefinition
    }

    private[config] def fieldDefinitionsSelect(tableName: String, version: Int) = {
        val etlDb = options.etlDb
        val hiveFieldsTableNameWithDb = SQLSyntax.createUnsafely(s"$etlDb.$fieldsConfigTableName")
        sql"""
               |SELECT
               |    *
               |FROM $hiveFieldsTableNameWithDb
               |WHERE HIVE_TABLE_NAME = $tableName
               |    AND CONFIG_VERSION = $version""".stripMargin
    }

    private[config] def extractFieldDefinition(rec: WrappedResultSet): FieldDefinition = {
        val regularFields = Seq("HIVE_TABLE_NAME",
            "CONFIG_VERSION",
            "HIVE_NAME",
            "SOURCE_NAME",
            "DESTINATION_NAME",
            "DATA_TYPE",
            "COLUMN_ORDER",
            "IS_NULL",
            "MISC",
            "INSERT_TIME")

        val hiveName = rec.string("HIVE_NAME")
        val columnName = rec.string("SOURCE_NAME")
        val destinationName = rec.string("DESTINATION_NAME")
        val dataType = rec.string("DATA_TYPE")
        val order = rec.int("COLUMN_ORDER")
        val isNull = rec.boolean("IS_NULL")
        val dataTypeDefinition = DataTypeParser.dataTypeFromString(dataType)
        val misc = JsonParser.jsonToAnyMap(rec.stringOpt("MISC").getOrElse(""))
        val insertTime = rec.timestamp("INSERT_TIME")

        val recMap = rec.toMap()
        val irregularFieldsMap = recMap.filterKeys(columnName => !regularFields.map(_.toUpperCase).contains(columnName.toUpperCase))

        FieldDefinition(
            columnName,
            hiveName,
            destinationName,
            dataTypeDefinition,
            order,
            isNull,
            misc ++ irregularFieldsMap,
            insertTime
        )
    }
}
