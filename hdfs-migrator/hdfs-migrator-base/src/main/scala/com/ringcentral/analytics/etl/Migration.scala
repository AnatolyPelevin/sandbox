package com.ringcentral.analytics.etl

import java.time.format.DateTimeParseException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

import com.ringcentral.analytics.etl.config.TableDefinition
import com.ringcentral.analytics.etl.fs.FileSystemService
import com.ringcentral.analytics.etl.logger.EtlLogger
import com.ringcentral.analytics.etl.migrator.FilterByDate
import com.ringcentral.analytics.etl.migrator.MigratorJobFactory
import com.ringcentral.analytics.etl.options.MigratorOptions
import com.ringcentral.analytics.etl.progress.ProgressLogger
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class Migration(implicit etlLogger: EtlLogger,
                spark: SparkSession,
                hive: Hive,
                options: MigratorOptions,
                fileSystem: FileSystemService,
                progressLogger: ProgressLogger) {

    private val log = LoggerFactory.getLogger(classOf[Migration])

    def execute(tables: Seq[TableDefinition]): Unit = {
        val tableNames: String = tables.map(_.hiveTableName).mkString(",")

        if (!isNewSchemeUsed(hive, options, tables)) {
            throw new RuntimeException("Please run migration after etl with new logic finished for first time")
        }

        log.info(s"Will migrate tables $tableNames")
        try {
            implicit val filter: FilterByDate = FilterByDate(options.startDate, options.endDate)

            val factory = new MigratorJobFactory

            val threadPool = Executors.newFixedThreadPool(options.jobsThreadsCount)
            val futures = tables
                .map(factory.createJob)
                .map(job => CompletableFuture.runAsync(job, threadPool))

            CompletableFuture.allOf(futures: _*).join()
            progressLogger.flush()
            spark.stop()
            threadPool.shutdownNow()

        } catch {
            case _: DateTimeParseException => log.error("Illegal date-start or date-end argument")
        }
    }

    private def isNewSchemeUsed(hive: Hive, options: MigratorOptions, tables: Seq[TableDefinition]): Boolean = {
        tables.filter(t => !t.isPartitioned)
            .map(t => hive.getTable(options.hiveDBName, t.hiveTableName).getDataLocation)
            .forall(location => location.toString.split("/").last.startsWith("ts="))
    }

}
