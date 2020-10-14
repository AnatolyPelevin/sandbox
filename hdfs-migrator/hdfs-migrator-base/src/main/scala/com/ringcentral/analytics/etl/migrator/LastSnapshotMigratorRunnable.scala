package com.ringcentral.analytics.etl.migrator

import java.time.LocalDateTime
import java.time.ZoneOffset

import com.ringcentral.analytics.etl.config.model.TableDefinition
import com.ringcentral.analytics.etl.fs.FileSystemService
import com.ringcentral.analytics.etl.logger.EtlLogger
import com.ringcentral.analytics.etl.options.MigratorOptions
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class LastSnapshotMigratorRunnable(tableConfig: TableDefinition)
                                  (implicit etlLogger: EtlLogger,
                                   spark: SparkSession,
                                   hive: Hive,
                                   options: MigratorOptions,
                                   fileSystem: FileSystemService
                                  ) extends Runnable with Logging {
    def run(): Unit = {
        val dbName = options.hiveDBName
        val tableName = tableConfig.hiveTableName
        val outDataPath = options.outDataPath
        val originalTablePath = s"$outDataPath/$tableName"
        val jobType = tableConfig.hiveTableName.toUpperCase
        logInfo(s"Initiate migration for table $tableName")

        logInfo(s"Check current location for table $tableName")
        val location = hive.getTable(dbName, tableName).getDataLocation.toString
        val hasTsInPath = location.split("/").last.startsWith("ts=")
        if (hasTsInPath) {
            logInfo(s"table $tableName already migrated. Location: $location")
            return
        }

        logInfo(s"Start moving data for table $tableName")

        val jobStartTime = etlLogger.getLastJobStartTime(jobType).getOrElse(LocalDateTime.now)
        val tsPath = s"ts=${jobStartTime.toInstant(ZoneOffset.UTC).toEpochMilli}"

        if (!fileSystem.moveFolder(tableName, originalTablePath, tsPath)) {
            logError(s"Migration for $tableName with location $location failed. Manual fix required")
            return
        }
        if (executeSqlWithLogging(alterTableLocationSql(tableName, tsPath))) {
            logInfo(s"Migration for table $tableName finished successfully")
        } else {
            logError(s"Migration for table $tableName failed")
        }
    }


    private def alterTableLocationSql(tableName: String, tsPath: String): String = {
        s"""
             |  ALTER TABLE ${options.hiveDBName}.${tableConfig.hiveTableName}
             | SET LOCATION '${options.outDataPath}/$tableName/$tsPath'
             | """.stripMargin
    }

    private def executeSqlWithLogging(sql: String): Boolean = {
        try {
            logInfo(s"Executing sql query $sql")
            spark.sql(sql)
            logInfo("Sql execution completed")
            true
        } catch {
            case e: Exception =>
                logError(s"${e.getMessage}")
                false
        }
    }
}
