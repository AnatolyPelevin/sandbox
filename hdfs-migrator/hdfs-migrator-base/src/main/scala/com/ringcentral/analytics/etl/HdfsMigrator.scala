package com.ringcentral.analytics.etl

import java.time.LocalDateTime
import java.time.ZoneOffset

import com.ringcentral.analytics.etl.config.ConfigReader
import com.ringcentral.analytics.etl.fs.FileSystemService
import com.ringcentral.analytics.etl.json.JsonStore
import com.ringcentral.analytics.etl.logger.EtlLogger
import com.ringcentral.analytics.etl.options.EtlLoggerOptions
import com.ringcentral.analytics.etl.options.MigratorOptions
import com.ringcentral.analytics.etl.progress.ProgressLogger
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object HdfsMigrator {
    private val LOG = LoggerFactory.getLogger(HdfsMigrator.getClass)


    def main(args: Array[String]): Unit = {

        implicit val options: MigratorOptions = MigratorOptions(args)
        LOG.info(options.toString)

        val sparkOptions = options.sparkOptions
        implicit val spark: SparkSession = SparkSession.builder
            .enableHiveSupport()
            .appName(options.applicationName)
            .config("spark.scheduler.mode", sparkOptions.schedulerMode)
            .config("spark.executor.memory", sparkOptions.executorMemory)
            .config("spark.driver.memory", sparkOptions.driverMemory)
            .getOrCreate()

        val hiveConf = new HiveConf(SparkHadoopUtil.get.conf, HdfsMigrator.getClass)
        implicit val hive: Hive = Hive.get(hiveConf)

        val loggerOptions: EtlLoggerOptions = options.etlLoggerOptions.copy(jobId = spark.sparkContext.applicationId)
        implicit val etlLogger: EtlLogger = new EtlLogger(loggerOptions, options.isSfdcMigration)

        val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        implicit val fileSystemService: FileSystemService = new FileSystemService(fileSystem)

        if (options.migrationLog.isEmpty) {
            val ts = LocalDateTime.now.toInstant(ZoneOffset.UTC).toEpochMilli
            val filename = s"./migration.$ts.json"

            implicit val progressLogger: ProgressLogger = ProgressLogger(JsonStore(filename))
            val configReader = new ConfigReader(options.etlLoggerOptions.dbConnectionOptions, options.tableConfigName, options.formulaConfigTableName)

            new Migration().execute(configReader.getTableConfigs)
        } else {
            new Rollback(JsonStore(options.migrationLog)).execute()
        }
    }
}
