package com.ringcentral.analytics.sns.request.generator

import scopt.OptionParser

case class GeneratorOptions(requestType: String = "",
                            partitionSpec: String = "",
                            schemaName: String = "",
                            tableNames: Seq[String] = Seq(),
                            snsUserName: String = "",
                            snsUserPassword: String = "",
                            snsHost: String = "",
                            snsPort: String = "",
                            hdfsPrefix: String = "hdfs://nameservice1",
                            snsControllerPath: String = "/notification",
                            connectionTimeout: Int = 10000)

object GeneratorOptions {
    private val parser: OptionParser[GeneratorOptions] = new scopt.OptionParser[GeneratorOptions]("Generator options") {
        head("Generator options")

        opt[String]("requestType").required().action((x, c) => c.copy(requestType = x)).text("Request type (APPEND|DELETE|REPLACE)")
        opt[String]("partitionSpec").required().action((x, c) => c.copy(partitionSpec = x)).text("RegExp for partitions to match with")
        opt[String]("schemaName").required().action((x, c) => c.copy(schemaName = x)).text("Schema name")
        opt[Seq[String]]("tableNames").required().action((x, c) => c.copy(tableNames = x)).text("Table names")
        opt[String]("snsUserName").required().action((x, c) => c.copy(snsUserName = x)).text("User name in SNS")
        opt[String]("snsUserPassword").required().action((x, c) => c.copy(snsUserPassword = x)).text("User password in SNS")
        opt[String]("snsHost").required().action((x, c) => c.copy(snsHost = x)).text("Host URL in SNS")
        opt[String]("snsPort").required().action((x, c) => c.copy(snsPort = x)).text("Port in SNS")
        opt[String]("hdfsPrefix").optional().action((x, c) => c.copy(hdfsPrefix = x)).text("Hdfs URL prefix (to remove from source path); def='hdfs://nameservice1'")
        opt[String]("snsControllerPath").optional().action((x, c) => c.copy(snsControllerPath = x)).text("SNS controller path (to intercept requests); def='/notification'")
        opt[Int]("connectionTimeout").optional().action((x, c) => c.copy(connectionTimeout = x)).text("Connection timeout; def=10000")

        help("help").text("prints usage text")
    }

    def apply(args: Array[String]): Option[GeneratorOptions] = {
        parser.parse(args, GeneratorOptions())
    }

    def getOptionsParser: OptionParser[GeneratorOptions] = parser
}