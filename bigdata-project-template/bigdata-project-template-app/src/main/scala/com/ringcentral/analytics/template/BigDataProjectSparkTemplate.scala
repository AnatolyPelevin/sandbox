package com.ringcentral.analytics.template

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession


object BigDataProjectSparkTemplate extends Logging{

    def main(args: Array[String]): Unit = {
        implicit val spark: SparkSession = SparkSession.builder
            .enableHiveSupport()
            .getOrCreate()
        logInfo("This is a BigData Project Spark Template")
    }
}
