package com.ringcentral.analytics.etl.options

case class SparkOptions(schedulerMode: String = "",
                        executorMemory: String = "1g",
                        driverMemory: String = "1g",
                        compression: String = "",
                        hiveExecMaxDynamicPartitions: Int = 0,
                        hiveExecMaxDynamicPartitionsPernode: Int = 0)
