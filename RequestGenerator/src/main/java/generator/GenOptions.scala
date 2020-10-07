package generator

import scopt.OptionParser

case class GenOptions(requestType: String = "",
                      partitionSpec: String = "",
                      schemaName: String = "",
                      tableNames: Seq[String] = Seq(),
                      snsUserName: String = "",
                      snsUserPassword: String = "",
                      snsHost: String = "",
                      snsPort: String = "")

object GenOptions {
    private val parser: OptionParser[GenOptions] = new scopt.OptionParser[GenOptions]("Generator options") {
        head("Generator options")

        opt[String]("requestType").action((x, c) => c.copy(requestType = x)).text("request type")
        opt[String]("partitionSpec").action((x, c) => c.copy(partitionSpec = x)).text("partition spec")
        opt[String]("schemaName").action((x, c) => c.copy(schemaName = x)).text("schema name")
        opt[Seq[String]]("tableNames").action((x, c) => c.copy(tableNames = x)).text("table names")
        opt[String]("snsUserName").action((x, c) => c.copy(snsUserName = x)).text("user name in SNS")
        opt[String]("snsUserPassword").action((x, c) => c.copy(snsUserPassword = x)).text("user password in SNS")
        opt[String]("snsHost").action((x, c) => c.copy(snsHost = x)).text("host URL in SNS")
        opt[String]("snsPort").action((x, c) => c.copy(snsPort = x)).text("port in SNS")

        help("help").text("prints usage text")
    }

    def apply(args: Array[String]): Option[GenOptions] = {
        parser.parse(args, GenOptions())
    }

    def getOptionsParser: OptionParser[GenOptions] = parser
}