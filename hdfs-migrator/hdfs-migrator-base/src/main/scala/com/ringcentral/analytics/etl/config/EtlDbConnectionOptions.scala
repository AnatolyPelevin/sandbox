package com.ringcentral.analytics.etl.config

case class EtlDbConnectionOptions(
                                     etlDbConnectionString: String = "",
                                     etlDb: String = "",
                                     etlDbUser: String = "",
                                     etlDbPassword: String = ""
                                 )
