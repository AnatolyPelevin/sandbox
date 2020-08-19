import java.sql.{Connection, DriverManager, PreparedStatement, Types}
import java.time.LocalDate
import java.util.Properties

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class VerticaOptions(verticaUrl: String,
                          verticaUser: String,
                          verticaPassword: String,
                          verticaDb: String) extends Serializable

case class FieldStruct(name: String,
                       hiveType: String,
                       verticaType: String) {
  val hiveFieldWithType: String = s"$name $hiveType"
  val verticaFieldWithType: String = s"$name $verticaType"
  val verticaTypeShort: String = ("[A-z]*".r findPrefixOf verticaType).getOrElse(verticaType).toLowerCase.trim
}

val accountLicensesFieldSpec: Seq[FieldStruct] = {
  Seq(
    FieldStruct("ngbs_account_id", "bigint", "bigint"),
    FieldStruct("first_name", "string", "varchar(400)"),
    FieldStruct("last_name", "string", "varchar(400)"),
    FieldStruct("company_name", "string", "varchar(400)"),
    FieldStruct("account_created", "timestamp", "timestamp"),
    FieldStruct("account_status", "bigint", "bigint"),
    FieldStruct("billing_codes_status", "bigint", "bigint"),
    FieldStruct("licenseid", "bigint", "bigint"),
    FieldStruct("parentid", "double", "double precision"),
    FieldStruct("license_added", "timestamp", "timestamp"),
    FieldStruct("elementid", "string", "varchar(1024)"),
    FieldStruct("categoryid", "string", "varchar(1024)"),
    FieldStruct("description", "string", "varchar(1024)"),
    FieldStruct("qty", "bigint", "bigint"),
    FieldStruct("billing_type", "bigint", "bigint"),
    FieldStruct("visible", "string", "varchar(1024)"),
    FieldStruct("duration", "bigint", "bigint"),
    FieldStruct("last_invoice_id", "double", "double precision"),
    FieldStruct("package_name", "string", "varchar(800)"),
    FieldStruct("provisioning_params", "string", "varchar(16000)"),
    FieldStruct("packageid", "bigint", "bigint"),
    FieldStruct("rc_account_id", "string", "varchar(256)"),
    FieldStruct("catalogid", "string", "varchar(256)"),
    FieldStruct("product_line", "string", "varchar(256)"),
    FieldStruct("product", "string", "varchar(256)"),
    FieldStruct("renewal_day", "bigint", "bigint"),
    FieldStruct("billing_start_date", "timestamp", "timestamp"),
    FieldStruct("previous_billing_date", "timestamp", "timestamp"),
    FieldStruct("next_billing_date", "timestamp", "timestamp"),
    FieldStruct("master_duration", "bigint", "bigint"),
    FieldStruct("currency", "string", "varchar(256)"),
    FieldStruct("package_ver_price_id", "bigint", "bigint"),
    FieldStruct("price", "double", "double precision"),
    FieldStruct("discount", "double", "double precision"),
    FieldStruct("amount", "double", "double precision"),
    FieldStruct("mrr", "double", "double precision"),
    FieldStruct("mrr_usd", "double", "double precision"),
    FieldStruct("package_version", "string", "varchar(256)"),
    FieldStruct("exchange_rate", "decimal(30,15)", "decimal(30,15)")
  )
}

val contractLicensesFieldSpec: Seq[FieldStruct] = {
  Seq(
    FieldStruct("rc_account_id", "string", "varchar(256)"),
    FieldStruct("ngbs_account_id", "bigint", "bigint"),
    FieldStruct("account_first_name", "string", "varchar(400)"),
    FieldStruct("account_last_name", "string", "varchar(400)"),
    FieldStruct("company_name", "string", "varchar(400)"),
    FieldStruct("account_created", "timestamp", "timestamp"),
    FieldStruct("account_status", "bigint", "bigint"),
    FieldStruct("licenseid", "bigint", "bigint"),
    FieldStruct("parentid", "double", "double precision"),
    FieldStruct("license_added", "timestamp", "timestamp"),
    FieldStruct("elementid", "string", "varchar(1024)"),
    FieldStruct("categoryid", "string", "varchar(1024)"),
    FieldStruct("license_description", "string", "varchar(1024)"),
    FieldStruct("duration", "bigint", "bigint"),
    FieldStruct("qty", "bigint", "bigint"),
    FieldStruct("billing_type", "bigint", "bigint"),
    FieldStruct("license_visible", "string", "varchar(1024)"),
    FieldStruct("last_invoice_id", "double", "double precision"),
    FieldStruct("packageid", "bigint", "bigint"),
    FieldStruct("package_name", "string", "varchar(800)"),
    FieldStruct("package_version", "string", "varchar(256)"),
    FieldStruct("product_line", "string", "varchar(256)"),
    FieldStruct("product", "string", "varchar(256)"),
    FieldStruct("tier", "string", "varchar(100)"),
    FieldStruct("rateplan", "string", "varchar(100)"),
    FieldStruct("billing_start_date", "timestamp", "timestamp"),
    FieldStruct("master_duration", "bigint", "bigint"),
    FieldStruct("currency", "string", "varchar(256)"),
    FieldStruct("catalogid", "string", "varchar(256)"),
    FieldStruct("product_id", "string", "varchar(200)"),
    FieldStruct("edition_id", "bigint", "bigint"),
    FieldStruct("offer_type_id", "bigint", "bigint"),
    FieldStruct("special_duration_unit", "bigint", "bigint"),
    FieldStruct("special_duration_value", "bigint", "bigint"),
    FieldStruct("package_ver_price_id", "bigint", "bigint"),
    FieldStruct("price", "double", "double precision"),
    FieldStruct("discount", "double", "double precision"),
    FieldStruct("amount", "double", "double precision"),
    FieldStruct("mrr", "double", "double precision"),
    FieldStruct("mrr_usd", "double", "double precision"),
    FieldStruct("contractid", "bigint", "bigint"),
    FieldStruct("contract_created", "timestamp", "timestamp"),
    FieldStruct("contract_start_date", "timestamp", "timestamp"),
    FieldStruct("term", "bigint", "bigint"),
    FieldStruct("contract_package_version", "string", "varchar(80)"),
    FieldStruct("contract_last_updated", "timestamp", "timestamp"),
    FieldStruct("contract_auto_renewal", "string", "varchar(4)"),
    FieldStruct("contract_renewal_term", "bigint", "bigint"),
    FieldStruct("contract_renewal_date", "timestamp", "timestamp"),
    FieldStruct("initial_sales_agreement", "string", "varchar(400)"),
    FieldStruct("is_active_initial_agreement", "string", "varchar(4)"),
    FieldStruct("sf_active_agreement_id", "string", "varchar(72)"),
    FieldStruct("sf_quote_number", "string", "varchar(120)"),
    FieldStruct("sf_quote_createddate", "timestamp", "timestamp"),
    FieldStruct("sf_quote_name", "string", "varchar(1020)"),
    FieldStruct("sf_quote_status", "string", "varchar(160)"),
    FieldStruct("sf_accountid", "string", "varchar(72)"),
    FieldStruct("sf_opportunityid", "string", "varchar(72)"),
    FieldStruct("sf_quote_start_date", "timestamp", "timestamp"),
    FieldStruct("sf_quote_end_date", "timestamp", "timestamp"),
    FieldStruct("sf_quote_ownerid", "string", "varchar(72)"),
    FieldStruct("sf_quote_ownername", "string", "varchar(484)"),
    FieldStruct("sf_quote_createdbyid", "string", "varchar(72)"),
    FieldStruct("sf_quote_createdbyname", "string", "varchar(484)"),
    FieldStruct("sf_quote_lastmodifieddate", "timestamp", "timestamp"),
    FieldStruct("sf_quote_lastmodifiedbyid", "string", "varchar(72)"),
    FieldStruct("sf_quote_lastmodifiedbyname", "string", "varchar(484)")
  )
}

def createConnection(options: VerticaOptions): Connection = {
  val connectionProperties = new Properties()
  connectionProperties.put("user", options.verticaUser)
  connectionProperties.put("password", options.verticaPassword)

  // Set streamingBatchInsert to True to enable streaming mode for batch inserts.
  connectionProperties.put("streamingBatchInsert", "true")

  // Enable directBatchInsert for this connection
  connectionProperties.put("DirectBatchInsert", "true")

  val conn = DriverManager.getConnection(options.verticaUrl, connectionProperties)
  conn.setAutoCommit(false)
  conn
}

val args = spark.sqlContext.getConf("spark.driver.args").split(",")

val verticaUrl = args(0)
val verticaUser = args(1)
val verticaPassword = args(2)
val verticaDb = args(3)
val dt = args(4)
val tableName = args(5)
val verticaOptions: VerticaOptions = VerticaOptions(
  verticaUrl,
  verticaUser,
  verticaPassword,
  verticaDb
)

def getFieldNames(fieldSpec: Seq[FieldStruct]): String = {
  fieldSpec.map(fs => s"`${fs.name}`").mkString(",")
}

def getStageTableName(tableName: String): String = {
  s"${tableName}_stage"
}

def createFieldNameSpec(spec: Seq[FieldStruct]): String = spec.map(_.name).mkString(", ")

def generateInsertVerticaQuery(tableName: String,
                               fields: Seq[FieldStruct],
                               verticaDB: String): String = {
  val questionMarks = fields
    .map(_ => "?")
    .mkString(",")

  s"""INSERT /*+ direct */  INTO $verticaDB.$tableName (${createFieldNameSpec(fields)})
     |VALUES($questionMarks)
     |""".stripMargin
}

def addPartitionSpec(fieldSpec: Seq[FieldStruct]): Seq[FieldStruct] = {
  fieldSpec ++ Seq(
    FieldStruct("dt", null, "timestamp not null")
  )
}

def getCurrentPartitionValue(date: LocalDate): String = {
  (date.getYear * 10000 + date.getMonthValue * 100 + date.getDayOfMonth).toString
}

val partitionColumn = "dt"

def getPartitionCondition: String = {
  s"DATE_PART('year', $partitionColumn) * 10000 + DATE_PART('month', $partitionColumn) * 100 +" +
    s" DATE_PART('day', $partitionColumn)"
}

def addPartitionColumn(dataFrame: DataFrame, date: LocalDate): DataFrame = {
  dataFrame.withColumn(partitionColumn, lit(date.toString).cast("timestamp"))
}

def dropPartition(tableName: String, partition: String, options: VerticaOptions): Unit = {
  val connection = createConnection(options)
  try {
    val statement = connection.createStatement()

    statement.execute(s"SELECT DROP_PARTITION('${options.verticaDb}.$tableName', '$partition')")

    connection.commit()
  } finally {
    connection.close()
  }
}

def swapPartitions(tableName: String, options: VerticaOptions, partition: String): Unit = {
  val connection = createConnection(options)
  try {
    val statement = connection.createStatement()

    val stageTableName = getStageTableName(tableName)

    val sql =
      s"""SELECT
         |SWAP_PARTITIONS_BETWEEN_TABLES(
         |'${options.verticaDb}.$stageTableName',
         |$partition,
         |$partition,
         |'${options.verticaDb}.$tableName'
         |)""".stripMargin
    statement.execute(sql)
    connection.commit()
  } finally {
    connection.close()
  }
}

def checkAndSet(row: Row, preparedStatement: PreparedStatement, position: Int, dataType: Int): Unit = {
  if (row.isNullAt(position)) {
    preparedStatement.setNull(position + 1, dataType)
  } else {
    dataType match {
      case Types.BIGINT => preparedStatement.setLong(position + 1, row.getLong(position))
      case Types.DOUBLE => preparedStatement.setDouble(position + 1, row.getDouble(position))
      case Types.BOOLEAN => preparedStatement.setBoolean(position + 1, row.getBoolean(position))
      case Types.INTEGER => preparedStatement.setInt(position + 1, row.getInt(position))
      case Types.TIMESTAMP => preparedStatement.setTimestamp(position + 1, row.getTimestamp(position))
      case Types.VARCHAR => preparedStatement.setString(position + 1, row.getString(position))
      case Types.DECIMAL => preparedStatement.setBigDecimal(position + 1, row.getDecimal(position))
      case unknownVerticaType: Int =>
        throw new MatchError(s"not supported Vertica type: $unknownVerticaType")
    }
  }
}

def addVerticaRowBatchToPstm(pstmt: PreparedStatement,
                             rows: Iterator[Row],
                             fields: Seq[FieldStruct]): Unit = {
  for {row <- rows} {
    fields.foreach(field => {
      val i = row.fieldIndex(field.name)
      val dataType = field.verticaTypeShort.toLowerCase match {
        case "bigint" => Types.BIGINT
        case "double" | "double precision" | "float" => Types.DOUBLE
        case "boolean" => Types.BOOLEAN
        case "int" | "integer" => Types.INTEGER
        case "timestamp" => Types.TIMESTAMP
        case "varchar" => Types.VARCHAR
        case "decimal" => Types.DECIMAL
        case unknownVerticaType: String =>
          throw new MatchError(s"not supported Vertica type: $unknownVerticaType")
      }

      checkAndSet(row, pstmt, i, dataType)
    })

    pstmt.addBatch()
  }
}


def truncateTable(tableName: String, options: VerticaOptions): Unit = {
  val connection = createConnection(options)
  try {
    val statement = connection.createStatement()

    statement.execute(s"TRUNCATE TABLE ${options.verticaDb}.$tableName")

    connection.commit()
  } finally {
    connection.close()
  }
}

def saveDataIntoTable(dataFrame: DataFrame,
                      tableName: String,
                      fields: Seq[FieldStruct],
                      options: VerticaOptions): Unit = {
  dataFrame.foreachPartition(rows => {
    val conn: Connection = createConnection(options)
    try {
      val pstmt = conn.prepareStatement(
        generateInsertVerticaQuery(tableName, fields, verticaDb)
      )

      addVerticaRowBatchToPstm(pstmt, rows, fields)

      pstmt.executeBatch()
      conn.commit()
    } finally {
      conn.close()
    }
  })
}

def savePartitioned(dataFrame: DataFrame,
                    fieldSpec: Seq[FieldStruct],
                    tableName: String,
                    snapshottedTableName: String,
                    umdSnapshottedStage: String,
                    date: LocalDate): Unit = {
  val partitionedSpec = addPartitionSpec(fieldSpec)
  val partitionValue = getCurrentPartitionValue(date)

  truncateTable(umdSnapshottedStage, verticaOptions)
  saveDataIntoTable(addPartitionColumn(dataFrame, date), umdSnapshottedStage, partitionedSpec, verticaOptions)
  dropPartition(snapshottedTableName, partitionValue, verticaOptions)
  swapPartitions(snapshottedTableName, verticaOptions, partitionValue)
  truncateTable(umdSnapshottedStage, verticaOptions)
}

def saveInVertica(dataFrame: DataFrame,
                  fieldSpec: Seq[FieldStruct],
                  tableName: String,
                  snapshottedTableName: String,
                  date: LocalDate): Unit = {
  val umdSnapshottedStage = getStageTableName(snapshottedTableName)

  savePartitioned(dataFrame, fieldSpec, tableName, snapshottedTableName, umdSnapshottedStage, date)
}

def saveIntoVertica(dataFrame: DataFrame, fieldSpec: Seq[FieldStruct], tableName: String): Unit = {
  val repartitionedDataFrame = dataFrame.coalesce(4)

  saveInVertica(repartitionedDataFrame,
    fieldSpec,
    tableName,
    tableName,
    LocalDate.parse(dt))
}

val fieldSpec = if (tableName == "umd6_contract_licenses_snapshotted") {
  contractLicensesFieldSpec
} else {
  accountLicensesFieldSpec
}

val queryHive =
  s"""SELECT ${getFieldNames(fieldSpec)}
     |  FROM ${verticaDb}.$tableName acc
     | WHERE acc.dt = '$dt'
     |""".stripMargin
val df = spark.sql(queryHive)

saveIntoVertica(df, fieldSpec, tableName)

System.exit(0)
