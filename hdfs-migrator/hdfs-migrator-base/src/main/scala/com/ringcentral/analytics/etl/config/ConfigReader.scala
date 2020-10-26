package com.ringcentral.analytics.etl.config

import scalikejdbc.ConnectionPool
import scalikejdbc.NamedAutoSession
import scalikejdbc.NamedDB

class ConfigReader(options: EtlDbConnectionOptions, tableName: String) {
    ConnectionPool.add("CONFIG_DB",
        options.etlDbConnectionString,
        options.etlDbUser,
        options.etlDbPassword)(ConnectionPool.DEFAULT_CONNECTION_POOL_FACTORY)
    implicit val session: NamedAutoSession = NamedAutoSession("CONFIG_DB")

    private lazy val allTableConfigs: Seq[TableDefinition] = readActiveTableConfigs

    def getTableConfigs: Seq[TableDefinition] = allTableConfigs


    private def readActiveTableConfigs: Seq[TableDefinition] = {
        NamedDB("CONFIG_DB") readOnly { implicit session =>
            new TableConfigReader(options, tableName).readActiveTableConfigs
        }
    }
}
