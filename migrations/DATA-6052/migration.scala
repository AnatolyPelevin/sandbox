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

object FieldNames {
  val id = "id"
  val catalogId = "catalogid"
  val version = "version"
  val ngbsAccountId = "ngbs_account_id"
  val added = "added"
  val description = "description"
  val productLine = "product_line"
  val product = "product"
  val accountid = "accountid"
  val renewalDay = "renewal_day"
  val gbcc = "gbcc"
  val billingStartDate = "billing_start_date"
  val nextBillingDate = "next_billing_date"
  val status = "status"
  val props = "props"
  val masterDuration = "master_duration"
  val previousBillingDate = "previous_billing_date"
  val externalSystemId = "external_system_id"
  val currency = "currency"
  val offerType = "offer_type"
  val durationUnit = "duration_unit"
  val durationValue = "duration_value"
  val targetCatalogid = "target_catalogid"
  val targetVersion = "target_version"
  val timezone = "timezone"
  val mrr = "mrr"
  val mrr1m = "mrr_1m"
  val mrr12m = "mrr_12m"
  val mrrDt = "mrr_dt"
  val mrrUsd = "mrr_usd"
  val mrrIncontact = "mrr_incontact"
  val mrrIncontactDt = "mrr_incontact_dt"
}

val fieldSpec: Seq[FieldStruct] = {
  import FieldNames._
  Seq(
    FieldStruct(id, "bigint", "bigint"),
    FieldStruct(catalogId, "string", "varchar(32)"),
    FieldStruct(version, "string", "varchar(16)"),
    FieldStruct(ngbsAccountId, "bigint", "bigint"),
    FieldStruct(added, "timestamp", "timestamp"),
    FieldStruct(description, "string", "varchar(1024)"),
    FieldStruct(productLine, "string", "varchar(16)"),
    FieldStruct(product, "string", "varchar(32)"),
    FieldStruct(accountid, "string", "varchar(64)"),
    FieldStruct(renewalDay, "bigint", "bigint"),
    FieldStruct(gbcc, "bigint", "bigint"),
    FieldStruct(billingStartDate, "timestamp", "timestamp"),
    FieldStruct(nextBillingDate, "timestamp", "timestamp"),
    FieldStruct(status, "bigint", "bigint"),
    FieldStruct(props, "string", "varchar(1024)"),
    FieldStruct(masterDuration, "bigint", "bigint"),
    FieldStruct(previousBillingDate, "timestamp", "timestamp"),
    FieldStruct(externalSystemId, "string", "varchar(32)"),
    FieldStruct(currency, "string", "varchar(32)"),
    FieldStruct(offerType, "bigint", "bigint"),
    FieldStruct(durationUnit, "bigint", "bigint"),
    FieldStruct(durationValue, "bigint", "bigint"),
    FieldStruct(targetCatalogid, "string", "varchar(32)"),
    FieldStruct(targetVersion, "string", "varchar(16)"),
    FieldStruct(timezone, "string", "varchar(64)"),
    FieldStruct(mrr, "double", "double precision"),
    FieldStruct(mrr1m, "double", "double precision"),
    FieldStruct(mrr12m, "double", "double precision"),
    FieldStruct(mrrDt, "timestamp", "timestamp"),
    FieldStruct(mrrUsd, "double", "double precision"),
    FieldStruct(mrrIncontact, "double", "double precision"),
    FieldStruct(mrrIncontactDt, "timestamp", "timestamp")
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

val spark = SparkSession.builder().appName("Ad-hoc migration for UMD, DATA-6052").enableHiveSupport().getOrCreate()

val dt = LocalDate.of(2020, 6, 28)

val args = spark.sqlContext.getConf("spark.driver.args").split(",")

val verticaUrl = args(0)
val verticaUser = args(1)
val verticaPassword = args(2)
val verticaDb = args(3)
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
        generateInsertVerticaQuery(tableName, fields, options.verticaDb)
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

def saveIntoVertica(dataFrame: DataFrame): Unit = {
  val repartitionedDataFrame = dataFrame.coalesce(4)

    val tableNameSnapshotted: String = "umd6_accounts_snapshotted"

  saveInVertica(repartitionedDataFrame,
    fieldSpec,
    "umd6_accounts",
    tableNameSnapshotted,
    dt)
}

val queryHive =
  s"""SELECT ${getFieldNames(fieldSpec)}
     |  FROM umd_production.umd6_accounts_snapshotted acc
     | WHERE acc.dt = '2020-06-28'
     |""".stripMargin
val df = spark.sql(queryHive)

saveIntoVertica(df)
