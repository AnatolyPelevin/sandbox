package com.ringcentral.analytics.etl.migrator

import java.time.LocalDate

import com.ringcentral.analytics.etl.fs.FileSystemService
import com.ringcentral.analytics.etl.options.MigratorOptions
import com.ringcentral.analytics.etl.progress.MigrationStep
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class PartitionedRollbackRunnable(migrationStep: MigrationStep)
                                 (implicit spark: SparkSession,
                                  options: MigratorOptions,
                                  fileSystem: FileSystemService) extends Runnable {
    private val log = LoggerFactory.getLogger(classOf[LastSnapshotMigratorRunnable])

    def run(): Unit = {
        val dt = migrationStep.dtPath.split("/").last.substring(3)
        val date: LocalDate = LocalDate.parse(dt)

        if (!fileSystem.moveFolderBack(migrationStep.tableName, migrationStep.dtPath, migrationStep.tsPath)) {
            log.error(s"Rollback for partition $dt failed. Manual fix required")
            return
        }

        if (!executeSqlWithLogging(alterPartitionTableLocationSql(migrationStep.tableName, date))) {
            log.error(s"Change location for partition $dt failed. Manual fix required")
            return
        }
        log.info(s"Partition $dt rollback was executed successfully.")
    }

    private def alterPartitionTableLocationSql(tableName: String, dt: LocalDate): String = {
        s"""
             |  ALTER TABLE ${options.hiveDBName}.$tableName PARTITION(dt='$dt')
             | SET LOCATION '${options.outDataPath}/$tableName/dt=$dt'
         """.stripMargin
    }


    private def executeSqlWithLogging(sql: String): Boolean = {
        try {
            log.info(s"Executing sql query $sql")
            spark.sql(sql)
            log.info("Sql execution completed")
            true
        } catch {
            case e: Exception =>
                log.error(s"${e.getMessage}")
                false
        }
    }
}
