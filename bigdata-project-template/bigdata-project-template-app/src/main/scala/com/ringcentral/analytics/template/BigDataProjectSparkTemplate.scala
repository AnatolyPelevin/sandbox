package com.ringcentral.analytics.template

import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory


object BigDataProjectSparkTemplate {

    private val log: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

    def main(args: Array[String]): Unit = {
        implicit val spark: SparkSession = SparkSession.builder
            .enableHiveSupport()
            .getOrCreate()
        log.info("This is a BigData Project Spark Template")
    }
}
