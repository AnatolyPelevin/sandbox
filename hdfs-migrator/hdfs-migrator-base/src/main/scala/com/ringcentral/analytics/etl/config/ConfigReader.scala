package com.ringcentral.analytics.etl.config

import com.ringcentral.analytics.etl.config.model.SourceDbDefinition
import com.ringcentral.analytics.etl.config.model.TableDefinition
import scalikejdbc.ConnectionPool
import scalikejdbc.NamedAutoSession
import scalikejdbc.NamedDB

class ConfigReader(options: EtlDbConnectionOptions, tableNames: TableConfigOptions) {
    ConnectionPool.add("CONFIG_DB",
        options.etlDbConnectionString,
        options.etlDbUser,
        options.etlDbPassword)(ConnectionPool.DEFAULT_CONNECTION_POOL_FACTORY)
    implicit val session: NamedAutoSession = NamedAutoSession("CONFIG_DB")

    private lazy val allTableConfigs: Seq[TableDefinition] = readActiveTableConfigs
    private lazy val sourceDbConfig: SourceDbDefinition = readSourceDbConfig

    def getTableConfigs: Seq[TableDefinition] = allTableConfigs

    def getSourceDbConfig: SourceDbDefinition = sourceDbConfig

    private def readActiveTableConfigs: Seq[TableDefinition] = {
        NamedDB("CONFIG_DB") readOnly { implicit session => {
            new TableConfigReader(options, tableNames.tableConfigTableName, tableNames.fieldConfigTableName).readActiveTableConfigs
        }
        }
    }

    private def readSourceDbConfig: SourceDbDefinition = {
        val reader = new SourceDbConfigReader(options, tableNames.dbConfigTableName)
        NamedDB("CONFIG_DB") readOnly { implicit session => {
            reader.readSourceDbDefinition()
        }
        }
    }

}
