package com.ringcentral.analytics.sns.backup.request.generator

import java.io.IOException
import java.time.LocalDate

import com.ringcentral.analytics.sns.backup.request.generator.model.{Message, RequestType}
import com.ringcentral.analytics.sns.backup.request.generator.utils.{MessageGenerator, RequestSender, SparkUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import sns.backup.request.generator.model.RequestType
import sns.backup.request.generator.utils.SparkUtils

object Generator extends Logging {
    implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)

    def main(args: Array[String]): Unit = {
        GeneratorOptions(args) match {
            case None => GeneratorOptions.getOptionsParser.showUsageAsError()
            case Some(o) =>
                implicit val options: GeneratorOptions = o
                implicit val spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()
                validateRequestType(options)
                generateAndSendRequests
        }
    }

    private def generateAndSendRequests(implicit options: GeneratorOptions, spark: SparkSession): Unit = {
        val sparkUtils = new SparkUtils(options, spark, options.schemaName)
        val messageGenerator = new MessageGenerator(options, sparkUtils)
        val requestSender = new RequestSender(options)

        for (tableName <- options.tableNames) {
            val messages = generateMessagesForTable(options, messageGenerator, tableName)
            sendTableMessages(requestSender, messages)
        }
        System.exit(0)
    }

    private def validateRequestType(options: GeneratorOptions): Unit = {
        try {
            RequestType.withName(options.requestType)
        } catch {
            case e: NoSuchElementException => {
                throw new NoSuchElementException(e.getMessage +
                    "\nRequest type is invalid: " +
                    options.requestType +
                    ". Allowed types are APPEND, DELETE or REPLACE")
            }
        }
    }

    def generateMessagesForTable(options: GeneratorOptions,
                                 messageGenerator: MessageGenerator,
                                 tableName: String): List[Message] = {
        val messages = messageGenerator.compose(tableName)
        if (messages.isEmpty) {
            throw new IllegalArgumentException("There are no partitions in table \"" + tableName + "\" matching spec: \"" + options.partitionSpec + "\"")
        }
        logInfo("Messages are generated for table \"" + options.schemaName + "." + tableName + "\"")
        messages
    }

    def sendTableMessages(requestSender: RequestSender,
                          messages: List[Message]): Unit = {
        for (message <- messages) {
            try {
                val response = requestSender.send(message)
                if (response != 200) {
                    val info = "Request for " + message.getSchemaName() + "." + message.getTableName() + "/" + message.getPartitionSpec() + " has not succeed: " + response
                    logInfo(info)
                }
            } catch {
                case e: IOException => {
                    logInfo("Request failed: ", e)
                }
            }
        }
        logInfo("Messages have been sent for table \"" + messages.head.getSchemaName() + "." + messages.head.getTableName() + "\"")
    }
}


