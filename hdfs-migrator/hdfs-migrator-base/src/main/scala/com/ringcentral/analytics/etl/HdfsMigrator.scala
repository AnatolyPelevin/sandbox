package com.ringcentral.analytics.etl

import java.time.format.DateTimeParseException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

import com.ringcentral.analytics.etl.config.ConfigReader
import com.ringcentral.analytics.etl.fs.FileSystemService
import com.ringcentral.analytics.etl.logger.EtlLogger
import com.ringcentral.analytics.etl.migrator.FilterByDate
import com.ringcentral.analytics.etl.migrator.MigratorJobFactory
import com.ringcentral.analytics.etl.options.EtlLoggerOptions
import com.ringcentral.analytics.etl.options.MigratorOptions
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
        implicit val etlLogger: EtlLogger = new EtlLogger(loggerOptions)

        val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        implicit val fileSystemService: FileSystemService = new FileSystemService(fileSystem)

        val configReader = new ConfigReader(options.etlLoggerOptions.dbConnectionOptions, options.tableConfigName)
        val tables = configReader.getTableConfigs

        val tableNames: String = tables.map(_.hiveTableName).mkString(",")
        LOG.info(s"Will migrate tables $tableNames")
        try {
            implicit val filter: FilterByDate = FilterByDate(options.startDate, options.endDate)

            val factory = new MigratorJobFactory

            val threadPool = Executors.newFixedThreadPool(options.jobsThreadsCount)
            val futures = tables
                .map(factory.createJob)
                .map(job => CompletableFuture.runAsync(job, threadPool))

            CompletableFuture.allOf(futures: _*).join()
            spark.stop()
            threadPool.shutdownNow()

        } catch {
            case _: DateTimeParseException => LOG.error("Illegal date-start or date-end argument")
        }
    }
}
