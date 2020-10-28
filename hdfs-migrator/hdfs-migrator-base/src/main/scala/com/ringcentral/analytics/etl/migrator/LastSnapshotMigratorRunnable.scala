package com.ringcentral.analytics.etl.migrator

import com.ringcentral.analytics.etl.config.TableDefinition
import com.ringcentral.analytics.etl.fs.FileSystemService
import com.ringcentral.analytics.etl.options.MigratorOptions
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.metadata.Hive
import org.slf4j.LoggerFactory

class LastSnapshotMigratorRunnable(tableConfig: TableDefinition)
                                  (implicit hive: Hive,
                                   options: MigratorOptions,
                                   fileSystem: FileSystemService
                                  ) extends Runnable {
    private val log = LoggerFactory.getLogger(classOf[LastSnapshotMigratorRunnable])

    def run(): Unit = {
        val dbName = options.hiveDBName
        val tableName = tableConfig.hiveTableName
        val outDataPath = options.outDataPath
        val originalTablePath = s"$outDataPath/$tableName"
        log.info(s"Initiate migration for table $tableName")

        log.info(s"Check current location for table $tableName")
        val location = hive.getTable(dbName, tableName).getDataLocation

        if (fileSystem.clearExcluding(new Path(originalTablePath), location)) {
            log.info(s"Migration for table $tableName finished successfully")
        } else {
            log.error(s"Migration for table $tableName failed")
        }
    }
}
