package generator.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

class SparkUtils (spark: SparkSession,
                  schemaName: String) {

    val hdfsPrefix = "hdfs://nameservice1"

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
                    println(e)
                    System.exit(1)
                }
            }
        }
        df.rdd
            .map(row => row.getString(0))
            .collect()
            .toList
    }

    def getSamplePartitionUrl(tableName: String, partition: String, isPartitioned: Boolean): String = {
        var df: DataFrame = spark.emptyDataFrame

        if (isPartitioned) {
            df = spark.sql(s"""describe formatted $schemaName.$tableName partition ($partition)""".stripMargin)
        } else {
            df = spark.sql(s"""describe formatted $schemaName.$tableName""".stripMargin)
        }

        df.filter(col("col_name").equalTo("Location"))
            .first()
            .getString(1)
            .replace(hdfsPrefix, "")
    }

}
