package com.ringcentral.analytics.etl.config

import scalikejdbc.ConnectionPool
import scalikejdbc.NamedAutoSession
import scalikejdbc.NamedDB

class ConfigReader(options: EtlDbConnectionOptions, tableName: String, formulaConfigTableName: String) {
    ConnectionPool.add("CONFIG_DB",
        options.etlDbConnectionString,
        options.etlDbUser,
        options.etlDbPassword)(ConnectionPool.DEFAULT_CONNECTION_POOL_FACTORY)
    implicit val session: NamedAutoSession = NamedAutoSession("CONFIG_DB")

    private lazy val allTableConfigs: Seq[TableDefinition] = readActiveTableConfigs

    def getTableConfigs: Seq[TableDefinition] = allTableConfigs


    private def readActiveTableConfigs: Seq[TableDefinition] = {
        NamedDB("CONFIG_DB") readOnly { implicit session =>
            val tableConfigReader = new TableConfigReader(options, tableName, formulaConfigTableName)
          val tableConfigs = tableConfigReader.readActiveTableConfigs
          val formulaTableConfigs = if (!formulaConfigTableName.isEmpty){
              tableConfigReader.readActiveFormulaTableConfigs
          } else {
              List.empty
          }
          tableConfigs ++ formulaTableConfigs
        }
    }
}
