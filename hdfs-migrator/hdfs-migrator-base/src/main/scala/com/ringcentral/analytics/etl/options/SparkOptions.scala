package com.ringcentral.analytics.etl.options

case class SparkOptions(schedulerMode: String = "",
                        compression: String = "",
                        hiveExecMaxDynamicPartitions: Int = 0,
                        hiveExecMaxDynamicPartitionsPernode: Int = 0)
