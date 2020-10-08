package com.ringcentral.analytics.sns.request.generator.utils

import com.ringcentral.analytics.sns.request.generator.GeneratorOptions
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

class SparkUtils (options: GeneratorOptions,
                  spark: SparkSession,
                  schemaName: String) {

    def getAllTablePartitions(tableName: String): List[String] = {
        var df: DataFrame = spark.emptyDataFrame
        try {
            df = spark.sql(s"""show partitions $schemaName.$tableName""")
        }
        catch {
            case e: Throwable => {
                if (e.getMessage.startsWith("SHOW PARTITIONS")) {
                    return List.empty
                } else {
                    throw e
                }
            }
        }
        df.rdd
            .map(row => row.getString(0))
            .collect()
            .toList
    }

    def getSourcePath(tableName: String, partition: String, isPartitioned: Boolean): String = {
        var df: DataFrame = spark.emptyDataFrame
        if (isPartitioned) {
            val partitionCorrected = getCorrectedPartition(partition)
            df = spark.sql(s"""describe formatted $schemaName.$tableName partition ($partitionCorrected)""".stripMargin)
        } else {
            df = spark.sql(s"""describe formatted $schemaName.$tableName""".stripMargin)
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
