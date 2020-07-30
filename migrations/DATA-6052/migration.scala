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

val fieldSpec: Seq[FieldStruct] = {
  Seq(
    FieldStruct("id", "bigint", "bigint"),
    FieldStruct("catalogid", "string", "varchar(32)"),
    FieldStruct("version", "string", "varchar(16)"),
    FieldStruct("ngbs_account_id", "bigint", "bigint"),
    FieldStruct("added", "timestamp", "timestamp"),
    FieldStruct("description", "string", "varchar(1024)"),
    FieldStruct("product_line", "string", "varchar(16)"),
    FieldStruct("product", "string", "varchar(32)"),
    FieldStruct("accountid", "string", "varchar(64)"),
    FieldStruct("renewal_day", "bigint", "bigint"),
    FieldStruct("gbcc", "bigint", "bigint"),
    FieldStruct("billing_start_date", "timestamp", "timestamp"),
    FieldStruct("next_billing_date", "timestamp", "timestamp"),
    FieldStruct("status", "bigint", "bigint"),
    FieldStruct("props", "string", "varchar(1024)"),
    FieldStruct("master_duration", "bigint", "bigint"),
    FieldStruct("previous_billing_date", "timestamp", "timestamp"),
    FieldStruct("external_system_id", "string", "varchar(32)"),
    FieldStruct("currency", "string", "varchar(32)"),
    FieldStruct("offer_type", "bigint", "bigint"),
    FieldStruct("duration_unit", "bigint", "bigint"),
    FieldStruct("duration_value", "bigint", "bigint"),
    FieldStruct("target_catalogid", "string", "varchar(32)"),
    FieldStruct("target_version", "string", "varchar(16)"),
    FieldStruct("timezone", "string", "varchar(64)"),
    FieldStruct("mrr", "double", "double precision"),
    FieldStruct("mrr_1m", "double", "double precision"),
    FieldStruct("mrr_12m", "double", "double precision"),
    FieldStruct("mrr_dt", "timestamp", "timestamp"),
    FieldStruct("mrr_usd", "double", "double precision"),
    FieldStruct("mrr_incontact", "double", "double precision"),
    FieldStruct("mrr_incontact_dt", "timestamp", "timestamp")
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

def saveIntoVertica(dataFrame: DataFrame): Unit = {
  val repartitionedDataFrame = dataFrame.coalesce(4)

    val tableNameSnapshotted: String = "umd6_accounts_snapshotted"

  saveInVertica(repartitionedDataFrame,
    fieldSpec,
    "umd6_accounts",
    tableNameSnapshotted,
    LocalDate.parse(dt))
}

val queryHive =
  s"""SELECT ${getFieldNames(fieldSpec)}
     |  FROM ${verticaDb}.umd6_accounts_snapshotted acc
     | WHERE acc.dt = '$dt'
     |""".stripMargin
val df = spark.sql(queryHive)

saveIntoVertica(df)
