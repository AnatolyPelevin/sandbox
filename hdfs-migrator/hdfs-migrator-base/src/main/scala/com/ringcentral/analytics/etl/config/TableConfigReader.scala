package com.ringcentral.analytics.etl.config

import com.ringcentral.analytics.etl.common.JsonParser
import com.ringcentral.analytics.etl.config.model.FieldDefinition
import com.ringcentral.analytics.etl.config.model.TableDefinition
import scalikejdbc.DBSession
import scalikejdbc.NoExtractor
import scalikejdbc.SQL
import scalikejdbc.WrappedResultSet
import scalikejdbc.interpolation.Implicits.scalikejdbcSQLInterpolationImplicitDef
import scalikejdbc.interpolation.SQLSyntax

class TableConfigReader(options: EtlDbConnectionOptions, tablesConfigTableName: String, fieldsConfigTableName: String) {

    def readActiveTableConfigs(implicit session: DBSession): Seq[TableDefinition] = {
        val tableConfigs: Seq[TableDefinition] = tableConfigsSelectSql.map(extractTableDefinition).list().apply()
        tableConfigs.map(t => new FieldConfigReader(options, fieldsConfigTableName).addToTableDefinition(t))
    }

    private def extractTableDefinition(rec: WrappedResultSet): TableDefinition = {
        val regularFields = Seq("HIVE_TABLE_NAME",
            "CONFIG_VERSION",
            "TABLE_QUERY",
            "DESTINATION_TABLE_NAME",
            "IS_PARTITIONED",
            "PRIORITY",
            "MISC",
            "ENABLED",
            "INSERT_TIME")

        val hiveTableName = rec.string("HIVE_TABLE_NAME")
        val version = rec.int("CONFIG_VERSION")
        val insertTime = rec.timestamp("INSERT_TIME")
        val tableQuery = rec.string("TABLE_QUERY")
        val destinationTableName = rec.stringOpt("DESTINATION_TABLE_NAME")
        val isSnapshotted = rec.boolean("IS_PARTITIONED")
        val priority = rec.int("PRIORITY")
        val misc = JsonParser.jsonToAnyMap(rec.stringOpt("MISC").getOrElse(""))

        val recMap = rec.toMap()
        val irregularFieldsMap = recMap.filterKeys(columnName => !regularFields.map(_.toUpperCase).contains(columnName.toUpperCase))

        TableDefinition(
            hiveTableName,
            version,
            insertTime,
            tableQuery,
            destinationTableName,
            isSnapshotted,
            priority,
            misc ++ irregularFieldsMap,
            Seq.empty[FieldDefinition]
        )
    }

    private def tableConfigsSelectSql: SQL[Nothing, NoExtractor] = {
        val configTableNameWithDb = s"${options.etlDb}.$tablesConfigTableName"
        val configTable = SQLSyntax.createUnsafely(configTableNameWithDb)
        sql"""
               |SELECT
               |    TC.*
               |FROM $configTable AS TC
               |INNER JOIN
               |(SELECT
               |    HIVE_TABLE_NAME, MAX(CONFIG_VERSION) as MAX_CONFIG_VERSION
               |  FROM $configTable GROUP BY HIVE_TABLE_NAME) AS TCG
               |ON (
               |    TC.HIVE_TABLE_NAME = TCG.HIVE_TABLE_NAME
               |    AND TC.CONFIG_VERSION = TCG.MAX_CONFIG_VERSION
               |      )
               |WHERE TC.ENABLED=TRUE
               |ORDER BY @TC.PRIORITY""".stripMargin
    }
}
