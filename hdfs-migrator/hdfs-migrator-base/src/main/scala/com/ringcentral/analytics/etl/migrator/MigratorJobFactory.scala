package com.ringcentral.analytics.etl.migrator

import com.ringcentral.analytics.etl.config.TableDefinition
import com.ringcentral.analytics.etl.fs.FileSystemService
import com.ringcentral.analytics.etl.logger.EtlLogger
import com.ringcentral.analytics.etl.options.MigratorOptions
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.sql.SparkSession

class MigratorJobFactory(implicit etlLogger: EtlLogger,
                         spark: SparkSession,
                         hive: Hive,
                         options: MigratorOptions,
                         fileSystem: FileSystemService,
                         dateFilter: FilterByDate) {

    def createJob(tableDefinition: TableDefinition): Runnable = {
        if (tableDefinition.isPartitioned) {
            return new PartitionedMigratorRunnable(tableDefinition)
        }
        new LastSnapshotMigratorRunnable(tableDefinition)
    }
}
