package com.ringcentral.analytics.etl.options

import com.ringcentral.analytics.etl.config.EtlDbConnectionOptions

case class EtlLoggerOptions(jobId: String = "",
                            dbConnectionOptions: EtlDbConnectionOptions = EtlDbConnectionOptions(),
                            etlLogTable: String = "",
                            lockWaitingTimeMs: Long = 0L,
                            lockCausedJobSuffix: String = "")
