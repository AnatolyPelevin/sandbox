package com.ringcentral.analytics.sns.request.generator.utils

import com.ringcentral.analytics.sns.request.generator.GeneratorOptions
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

class SparkUtils (options: GeneratorOptions,
                  spark: SparkSession,
                  schemaName: String) {

    def getAllTablePartitions(tableName: String): List[String] = {
        try {
            val df = spark.sql(s"show partitions $schemaName.$tableName")
            df.rdd
                .map(row => row.getString(0))
                .collect()
                .toList
        }
        catch {
            case e: Throwable => {
                if (e.getMessage.startsWith("SHOW PARTITIONS")) {
                    List.empty
                } else {
                    throw e
                }
            }
        }
    }

    def getSourcePath(tableName: String, partition: String, isPartitioned: Boolean): String = {
        val df: DataFrame = if (isPartitioned) {
            val partitionCorrected = getCorrectedPartition(partition)
            spark.sql(s"describe formatted $schemaName.$tableName partition ($partitionCorrected)")
        } else {
            spark.sql(s"describe formatted $schemaName.$tableName")
        }

        df.filter(col("col_name").equalTo("Location"))
            .first()
            .getString(1)
            .replace(options.hdfsPrefix, "")
    }

    def getCorrectedPartition(partition: String): String = {
        partition.replace("/", "',").replace("=", "='").concat("'")
    }
}
