package com.ringcentral.analytics.etl

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService

import com.ringcentral.analytics.etl.fs.FileSystemService
import com.ringcentral.analytics.etl.json.JsonStore
import com.ringcentral.analytics.etl.migrator.PartitionedRollbackRunnable
import com.ringcentral.analytics.etl.options.MigratorOptions
import com.ringcentral.analytics.etl.progress.MigrationStep
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class Rollback(jsonHelper: JsonStore)(implicit spark: SparkSession,
                                      options: MigratorOptions,
                                      fileSystem: FileSystemService) {
    private val log = LoggerFactory.getLogger(classOf[Rollback])

    def execute(): Unit = {
        val migrationSteps: Seq[MigrationStep] = jsonHelper.read()

        log.info("Start rollback process")

        val threadPool = Executors.newFixedThreadPool(options.jobsThreadsCount)
        val futures = migrationSteps.map(step => submit(threadPool, step))

        CompletableFuture.allOf(futures: _*).join()
        spark.stop()
        threadPool.shutdownNow()

    }

    private def submit(threadPool: ExecutorService, step: MigrationStep) = {
        CompletableFuture.runAsync(new PartitionedRollbackRunnable(step), threadPool)
    }
}
