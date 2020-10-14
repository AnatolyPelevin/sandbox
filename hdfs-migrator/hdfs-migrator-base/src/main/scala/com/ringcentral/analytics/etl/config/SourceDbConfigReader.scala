package com.ringcentral.analytics.etl.config

import com.ringcentral.analytics.etl.common.JsonParser
import com.ringcentral.analytics.etl.config.model.SourceDbDefinition
import scalikejdbc.DBSession
import scalikejdbc.NoExtractor
import scalikejdbc.SQL
import scalikejdbc.WrappedResultSet
import scalikejdbc.interpolation.Implicits.scalikejdbcSQLInterpolationImplicitDef
import scalikejdbc.interpolation.SQLSyntax

class SourceDbConfigReader(options: EtlDbConnectionOptions, dbConfigTableName: String) {

    def readSourceDbDefinition()(implicit session: DBSession): SourceDbDefinition = {
        val dbConfig = dbConfigsSelectSql.stripMargin.map(rec => extractSourceDbDefinition(rec)).single().apply()
        dbConfig.getOrElse(throw new MisconfigurationException("No db config settings"))
    }

    private[config] def extractSourceDbDefinition(rec: WrappedResultSet): SourceDbDefinition = {
        val regularFields = Seq("JDBC_URL", "USERNAME", "PASSWORD", "JDBC_DRIVER", "MISC", "CONFIG_VERSION", "DB_PROPERTIES", "INSERT_TIME")

        val jdbcUrl = rec.string("JDBC_URL")
        val jdbcUsername = rec.string("USERNAME")
        val jdbcPassword = rec.string("PASSWORD")
        val jdbcDriver = rec.string("JDBC_DRIVER")
        val version = rec.int("CONFIG_VERSION")
        val dbProperties: Map[String, String] = JsonParser.jsonToStringMap(rec.string("DB_PROPERTIES"))
        val misc: Map[String, Any] = JsonParser.jsonToAnyMap(rec.string("MISC"))

        val recMap = rec.toMap()
        val irregularFieldsMap = recMap.filterKeys(columnName => !regularFields.map(_.toUpperCase).contains(columnName.toUpperCase))

        SourceDbDefinition(
            jdbcUrl,
            jdbcUsername,
            jdbcPassword,
            jdbcDriver,
            version,
            dbProperties ++ Map("driver" -> jdbcDriver),
            misc ++ irregularFieldsMap
        )
    }

    private def dbConfigsSelectSql: SQL[Nothing, NoExtractor] = {

        val dbSettingsTableNameWithDb = s"${options.etlDb}.$dbConfigTableName"
        val dbSettingsTable = SQLSyntax.createUnsafely(dbSettingsTableNameWithDb)
        sql"""
               |SELECT
               |    DBC.JDBC_URL,
               |    DBC.USERNAME,
               |    DBC.PASSWORD,
               |    DBC.JDBC_DRIVER,
               |    DBC.CONFIG_VERSION,
               |    DBC.DB_PROPERTIES,
               |    DBC.MISC,
               |    DBC.INSERT_TIME
               |FROM $dbSettingsTable AS DBC
               |WHERE DBC.CONFIG_VERSION=(SELECT MAX(DBCG.CONFIG_VERSION)
               |                          FROM $dbSettingsTable AS DBCG)""".stripMargin
    }

}
