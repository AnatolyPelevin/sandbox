package com.ringcentral.analytics.etl.logger

import java.time.LocalDate
import java.time.LocalDateTime

import com.ringcentral.analytics.etl.options.EtlLoggerOptions
import org.apache.spark.internal.Logging
import scalikejdbc.ConnectionPool
import scalikejdbc.NamedAutoSession
import scalikejdbc.NamedDB
import scalikejdbc.SQLSyntax
import scalikejdbc.scalikejdbcSQLInterpolationImplicitDef

class EtlLogger(options: EtlLoggerOptions) extends Logging {
    ConnectionPool.add("ETL_DB",
        options.dbConnectionOptions.etlDbConnectionString,
        options.dbConnectionOptions.etlDbUser,
        options.dbConnectionOptions.etlDbPassword)(ConnectionPool.DEFAULT_CONNECTION_POOL_FACTORY)
    implicit val session: NamedAutoSession = NamedAutoSession("ETL_DB")
    private val fullTableName = SQLSyntax.createUnsafely(s"${options.dbConnectionOptions.etlDb}.${options.etlLogTable}")

    private[etl] def getFullTableName: SQLSyntax = {
        fullTableName
    }

    def getLastJobStartTime(jobType: String): Option[LocalDateTime] = {
        val query =
            sql"""
                SELECT job_start_time
                  FROM $getFullTableName
                 WHERE job_type = $jobType
                   AND status='FINISHED'
              ORDER BY JOB_START_TIME DESC
                 LIMIT 1
          """.stripMargin
        logInfo(s"executed query: ${query.statement}")
        NamedDB("ETL_DB") readOnly { implicit session =>
            query.map(_.timestamp("job_start_time"))
                .single()
                .apply()
                .map(_.toLocalDateTime)
        }
    }

    def getJobStartTimeForDate(jobType: String, localDate: LocalDate): Option[LocalDateTime] = {
        val query =
            sql"""
                SELECT job_start_time
                  FROM $getFullTableName
                 WHERE job_type = $jobType
                   AND status = 'FINISHED'
                   AND start_date::TIMESTAMP::date = $localDate
              ORDER BY JOB_START_TIME DESC
                 LIMIT 1
          """.stripMargin
        logInfo(s"executed query: ${query.statement}")
        NamedDB("ETL_DB") readOnly { implicit session =>
            query.map(_.timestamp("job_start_time")).single().apply().map(_.toLocalDateTime)
        }
    }
}
