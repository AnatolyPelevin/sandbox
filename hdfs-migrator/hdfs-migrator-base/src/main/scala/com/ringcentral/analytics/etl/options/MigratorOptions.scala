package com.ringcentral.analytics.etl.options

import java.time.LocalDate
import java.time.LocalDateTime

import com.ringcentral.analytics.etl.config.EtlDbConnectionOptions
import com.ringcentral.analytics.etl.config.TableConfigOptions
import joptsimple.OptionParser
import joptsimple.OptionSet

case class MigratorOptions(
                              etlLoggerOptions: EtlLoggerOptions = EtlLoggerOptions(),
                              sparkOptions: SparkOptions = SparkOptions(),
                              tableConfigOptions: TableConfigOptions = TableConfigOptions(),
                              outDataPath: String = "",
                              hiveDBName: String = "",
                              dt: LocalDateTime,
                              applicationName: String = "",
                              jobsThreadsCount: Int = 0) {
    def getCurrentDate: LocalDate = dt.toLocalDate
}

object MigratorOptions {

    private val DEFAULT_SCHEDULER_MODE = "FAIR"
    private val DEFAULT_JOBS_THREADS_COUNT = 10

    private val DB_CONFIG_TABLE_NAME = "db-config-table-name"
    private val TABLE_CONFIG_TABLE_NAME = "table-config-table-name"
    private val FIELD_CONFIG_TABLE_NAME = "field-config-table-name"

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

    def applyTableConfigOptions(options: OptionSet): TableConfigOptions = {
        val dbConfigTableName = options.valueOf(DB_CONFIG_TABLE_NAME).asInstanceOf[String]
        val tableConfigTableName = options.valueOf(TABLE_CONFIG_TABLE_NAME).asInstanceOf[String]
        val hiveFieldConfigTable = options.valueOf(FIELD_CONFIG_TABLE_NAME).asInstanceOf[String]
        TableConfigOptions(
            dbConfigTableName,
            tableConfigTableName,
            hiveFieldConfigTable
        )
    }

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
        SparkOptions(
            schedulerMode
        )
    }

    def applyEtlDbConnectionOptions(options: OptionSet): EtlDbConnectionOptions = {
        val etlDbDatabase = options.valueOf(ETLDB_DATABASE).asInstanceOf[String]
        val etldbConnectionString = options.valueOf(ETLDB_CONNECTION_STRING).asInstanceOf[String]
        val etlDbUser = options.valueOf(ETLDB_USER).asInstanceOf[String]
        val etlDbPassword = options.valueOf(ETLDB_PASSWORD).asInstanceOf[String]
        EtlDbConnectionOptions(
            etldbConnectionString,
            etlDbDatabase,
            etlDbUser,
            etlDbPassword)
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
        accepts(DB_CONFIG_TABLE_NAME).withRequiredArg()
        accepts(TABLE_CONFIG_TABLE_NAME).withRequiredArg()
        accepts(FIELD_CONFIG_TABLE_NAME).withRequiredArg()
    }

    def apply(args: Array[String]): MigratorOptions = {
        val options = PARSER.parse(args: _*)

        val etlDbConnectionOptions = applyEtlDbConnectionOptions(options)

        MigratorOptions(
            applyEtlLoggerOptions(options, etlDbConnectionOptions),
            applySparkOptions(options),
            applyTableConfigOptions(options),
            options.valueOf(OUT_DATA_PATH).asInstanceOf[String],
            options.valueOf(HIVE_DB_NAME).asInstanceOf[String],
            LocalDateTime.now(),
            options.valueOf(APPLICATION_NAME).asInstanceOf[String],
            options.valueOf(JOBS_THREADS_COUNT).asInstanceOf[Int])
    }
}
