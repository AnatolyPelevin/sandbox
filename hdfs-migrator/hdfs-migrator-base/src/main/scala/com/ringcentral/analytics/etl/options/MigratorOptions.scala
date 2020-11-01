package com.ringcentral.analytics.etl.options

import com.ringcentral.analytics.etl.config.EtlDbConnectionOptions
import joptsimple.OptionParser
import joptsimple.OptionSet

case class MigratorOptions(
                              etlLoggerOptions: EtlLoggerOptions = EtlLoggerOptions(),
                              sparkOptions: SparkOptions = SparkOptions(),
                              tableConfigName: String = "",
                              outDataPath: String = "",
                              hiveDBName: String = "",
                              applicationName: String = "",
                              jobsThreadsCount: Int = 0,
                              startDate: String = "",
                              endDate: String = "",
                              isSfdcMigration: Boolean = false,
                              formulaConfigTableName: String = "")

object MigratorOptions {

    private val DEFAULT_SCHEDULER_MODE = "FAIR"
    private val DEFAULT_JOBS_THREADS_COUNT = 10

    private val TABLE_CONFIG_TABLE_NAME = "table-config-table-name"

    private val OUT_DATA_PATH = "out-data-path"
    private val JOBS_THREADS_COUNT = "jobs-threads-count"
    private val HIVE_DB_NAME = "hive-db-name"
    private val ETLDB_DATABASE = "etldb-database"
    private val ETLDB_CONNECTION_STRING = "etldb-connection-string"
    private val ETLDB_USER = "etldb-user"
    private val ETLDB_PASSWORD = "etldb-password"
    private val APPLICATION_NAME = "application-name"
    private val SCHEDULER_MODE = "scheduler-mode"
    private val ETL_LOG_TABLE = "etl-log-table"
    private val DRIVER_MEMORY = "driver-memory"
    private val EXECUTOR_MEMORY = "executor-memory"
    private val START_DATE = "start-date"
    private val END_DATE = "end-date"
    private val IS_SFDC_MIGRATION = "is-sfdc-migration"
    private val FORMULA_CONFIG_TABLE_NAME = "formula-config-table-name"

    def applyEtlLoggerOptions(options: OptionSet, etlDbConnectionOptions: EtlDbConnectionOptions): EtlLoggerOptions = {
        val etlLogTable = options.valueOf(ETL_LOG_TABLE).asInstanceOf[String]

        EtlLoggerOptions(
            "",
            etlDbConnectionOptions,
            etlLogTable
        )
    }

    def applySparkOptions(options: OptionSet): SparkOptions = {
        val schedulerMode = options.valueOf(SCHEDULER_MODE).asInstanceOf[String]
        val executorMemory = options.valueOf(EXECUTOR_MEMORY).asInstanceOf[String]
        val driverMemory = options.valueOf(DRIVER_MEMORY).asInstanceOf[String]
        SparkOptions(schedulerMode, executorMemory, driverMemory)
    }

    def applyEtlDbConnectionOptions(options: OptionSet): EtlDbConnectionOptions = {
        val etlDbDatabase = options.valueOf(ETLDB_DATABASE).asInstanceOf[String]
        val etldbConnectionString = options.valueOf(ETLDB_CONNECTION_STRING).asInstanceOf[String]
        val etlDbUser = options.valueOf(ETLDB_USER).asInstanceOf[String]
        val etlDbPassword = options.valueOf(ETLDB_PASSWORD).asInstanceOf[String]
        EtlDbConnectionOptions(etldbConnectionString, etlDbDatabase, etlDbUser, etlDbPassword)
    }

    private val PARSER = new OptionParser {
        accepts(OUT_DATA_PATH).withRequiredArg()
        accepts(JOBS_THREADS_COUNT).withOptionalArg().ofType(classOf[Int]).defaultsTo(DEFAULT_JOBS_THREADS_COUNT)
        accepts(HIVE_DB_NAME).withRequiredArg()
        accepts(ETLDB_DATABASE).withRequiredArg()
        accepts(ETLDB_CONNECTION_STRING).withRequiredArg()
        accepts(ETLDB_USER).withRequiredArg()
        accepts(ETLDB_PASSWORD).withRequiredArg()
        accepts(APPLICATION_NAME).withRequiredArg()
        accepts(SCHEDULER_MODE).withOptionalArg().defaultsTo(DEFAULT_SCHEDULER_MODE)
        accepts(ETL_LOG_TABLE).withRequiredArg()
        accepts(TABLE_CONFIG_TABLE_NAME).withRequiredArg()
        accepts(DRIVER_MEMORY).withOptionalArg().defaultsTo("1g")
        accepts(EXECUTOR_MEMORY).withOptionalArg().defaultsTo("1g")
        accepts(START_DATE).withOptionalArg().defaultsTo("")
        accepts(END_DATE).withOptionalArg().defaultsTo("")
        accepts(IS_SFDC_MIGRATION).withRequiredArg().ofType(classOf[Boolean]).defaultsTo(false)
        accepts(FORMULA_CONFIG_TABLE_NAME).withOptionalArg().defaultsTo("")
    }

    def apply(args: Array[String]): MigratorOptions = {
        val options = PARSER.parse(args: _*)

        val etlDbConnectionOptions = applyEtlDbConnectionOptions(options)

        MigratorOptions(
            applyEtlLoggerOptions(options, etlDbConnectionOptions),
            applySparkOptions(options),
            options.valueOf(TABLE_CONFIG_TABLE_NAME).asInstanceOf[String],
            options.valueOf(OUT_DATA_PATH).asInstanceOf[String],
            options.valueOf(HIVE_DB_NAME).asInstanceOf[String],
            options.valueOf(APPLICATION_NAME).asInstanceOf[String],
            options.valueOf(JOBS_THREADS_COUNT).asInstanceOf[Int],
            options.valueOf(START_DATE).asInstanceOf[String],
            options.valueOf(END_DATE).asInstanceOf[String],
            options.valueOf(IS_SFDC_MIGRATION).asInstanceOf[Boolean],
            options.valueOf(FORMULA_CONFIG_TABLE_NAME).asInstanceOf[String]
        )
    }
}
