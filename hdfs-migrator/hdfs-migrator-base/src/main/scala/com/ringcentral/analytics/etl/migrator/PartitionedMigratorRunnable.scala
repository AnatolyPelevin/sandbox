package com.ringcentral.analytics.etl.migrator

import java.time.LocalDate
import java.time.ZoneOffset
import java.util

import com.ringcentral.analytics.etl.config.model.TableDefinition
import com.ringcentral.analytics.etl.fs.FileSystemService
import com.ringcentral.analytics.etl.logger.EtlLogger
import com.ringcentral.analytics.etl.options.MigratorOptions
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.metadata.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions.asScalaBuffer

class PartitionedMigratorRunnable(tableConfig: TableDefinition)
                                 (implicit etlLogger: EtlLogger,
                                  spark: SparkSession,
                                  hive: Hive,
                                  options: MigratorOptions,
                                  fileSystem: FileSystemService
                                 ) extends Runnable with Logging {

    def run(): Unit = {
        val dbName = options.hiveDBName
        val tableName = tableConfig.hiveTableName
        logInfo(s"Initiate migration for table $tableName")

        val table = hive.getTable(dbName, tableName)
        val partitions: util.List[Partition] = hive.getPartitions(table)

        val existingPartitionLocations: Array[String] = partitions.map(_.getLocation).toArray

        logInfo(s"Existing partitions locations: ${existingPartitionLocations.mkString(",")}")

        if (isMigrationComplete(existingPartitionLocations)) {
            logInfo(s"Table $tableName already migrated.")
            return
        }

        if (existingPartitionLocations.count(isMigratedPartition) > 0) {
            logInfo(s"Some of partitions table $tableName already migrated but not all.")
            logInfo("Start migration for remaining partitions.")
        }

        val success = existingPartitionLocations.filter(!isMigratedPartition(_))
            .forall(location => migrate(location, tableName))
        if (success) {
            logInfo(s"Migration for table $tableName finished successfully")
        } else {
            logWarning(s"Migration for table $tableName finished with errors. Some partitions are not migrated")
        }
    }

    def migrate(location: String, tableName: String): Boolean = {
        logInfo(s"Start migration for $tableName partition with location $location")
        val jobType = tableConfig.hiveTableName.toUpperCase

        val date: LocalDate = LocalDate.parse(location.split("/").last.substring(3))
        val dateTime = etlLogger.getJobStartTimeForDate(jobType, date).getOrElse(date.atStartOfDay())

        val tsPath = s"ts=${dateTime.toInstant(ZoneOffset.UTC).toEpochMilli}"

        if (!fileSystem.moveFolder(tableName, location, tsPath)) {
            logError(s"Migration for $tableName partition with location $location failed. Manual fix required")
            return false
        }

        executeSqlWithLogging(alterPartitionTableLocationSql(tableName, date, tsPath))
    }


    private def isMigrationComplete(locations: Array[String]) = {
        locations.length == locations.count(isMigratedPartition)
    }

    private def isMigratedPartition(location: String) = {
        location.split("/").last.startsWith("ts=")
    }

    private def alterPartitionTableLocationSql(tableName: String, dt: LocalDate, tsPath: String): String = {
        s"""
             |  ALTER TABLE ${options.hiveDBName}.$tableName PARTITION(dt='$dt')
             | SET LOCATION '${options.outDataPath}/$tableName/dt=$dt/$tsPath'
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
