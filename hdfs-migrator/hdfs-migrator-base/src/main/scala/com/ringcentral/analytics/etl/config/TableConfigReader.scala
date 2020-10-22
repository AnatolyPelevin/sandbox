package com.ringcentral.analytics.etl.config

import com.ringcentral.analytics.etl.config.model.TableDefinition
import scalikejdbc.DBSession
import scalikejdbc.NoExtractor
import scalikejdbc.SQL
import scalikejdbc.WrappedResultSet
import scalikejdbc.interpolation.Implicits.scalikejdbcSQLInterpolationImplicitDef
import scalikejdbc.interpolation.SQLSyntax

class TableConfigReader(options: EtlDbConnectionOptions, tablesConfigTableName: String) {

    def readActiveTableConfigs(implicit session: DBSession): Seq[TableDefinition] = {
        tableConfigsSelectSql.map(extractTableDefinition).list().apply()
    }

    private def extractTableDefinition(rec: WrappedResultSet): TableDefinition = {
        val hiveTableName = rec.string("HIVE_TABLE_NAME")
        val isSnapshotted = rec.boolean("IS_PARTITIONED")

        TableDefinition(hiveTableName, isSnapshotted)
    }

    private def tableConfigsSelectSql: SQL[Nothing, NoExtractor] = {
        val configTableNameWithDb = s"${options.etlDb}.$tablesConfigTableName"
        val configTable = SQLSyntax.createUnsafely(configTableNameWithDb)
        sql"""
               |    SELECT TC.HIVE_TABLE_NAME, TC.IS_PARTITIONED
               |      FROM $configTable AS TC
               |INNER JOIN (SELECT HIVE_TABLE_NAME, MAX(CONFIG_VERSION) as MAX_CONFIG_VERSION
               |              FROM $configTable GROUP BY HIVE_TABLE_NAME) AS TCG
               |        ON (TC.HIVE_TABLE_NAME = TCG.HIVE_TABLE_NAME AND TC.CONFIG_VERSION = TCG.MAX_CONFIG_VERSION)
               |     WHERE TC.ENABLED=TRUE
               |  ORDER BY @TC.PRIORITY""".stripMargin
    }
}
