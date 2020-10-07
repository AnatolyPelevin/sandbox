package generator

import java.time.LocalDate

import generator.model.RequestType
import generator.utils.{MessageGenerator, RequestSender, SparkUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object Generator extends Logging {
    implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)

    def main(args: Array[String]): Unit = {
        GenOptions(args) match {
            case None => GenOptions.getOptionsParser.showUsageAsError()
            case Some(o) =>
                implicit val options: GenOptions = o
                implicit val spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()
                generate
        }
    }

    private def generate(implicit options: GenOptions, spark: SparkSession): Unit = {

        val sparkUtils = new SparkUtils(spark, options.schemaName)
        val messageList = new MessageGenerator(options, sparkUtils)
        val requestSender = new RequestSender(options)

        if (!isValidRequestType(options)) {
            println("REQUEST TYPE IS INVALID: " + options.requestType)
            System.exit(1)
        }

        for (tableName <- options.tableNames) {
            val messages = messageList.compose(tableName)

            if (messages.isEmpty) {
                println("THERE ARE NO PARTITIONS MATCHING SPEC: \"" + options.partitionSpec + "\" FOR TABLE: " + tableName)
                System.exit(1)
            }
            messages.foreach(mes => println(mes + ": " + requestSender.send(mes)))
        }
        System.exit(0)
    }

    private def isValidRequestType(options: GenOptions): Boolean = {
        try {
            RequestType.valueOf(options.requestType)
        }catch {
            case _: IllegalArgumentException => {
                return false
            }
        }
        true
    }
}


