import java.io.File
import org.apache.spark.sql._
import spark.sql
import java.time.{LocalDate, LocalDateTime}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType

val warehouseLocation = new File("spark-warehouse").getAbsolutePath
val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()

val args = spark.sqlContext.getConf("spark.driver.args").split(",")
val sourseDB = args(0)
val destinationDB= args(1)
val startDate = LocalDate.parse(args(2))
val endDate = LocalDate.parse(args(3))
val tableName = args(4)

val writeMode = "ignore"

def processAbentrylog(dateslist: List[LocalDate], source: String, destination: String): Unit = {
	for (dt <- dateslist){
		val dateStr = dt.toString

		println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

		val df = sql(s"select * from $source.abentrylog where year='${dt.getYear}' and month='${dt.getMonth}' and day='${dt.getDayOfMonth}'")

		val newDf = df.withColumn("abentryid", 'abentryid.cast(LongType))
									.withColumn("fmcounter", 'fmcounter.cast(LongType))
									.withColumn("mailboxid", 'mailboxid.cast(LongType))
									.withColumn("entrytype", 'entrytype.cast(IntegerType))
									.withColumn("objecttype", 'objecttype.cast(IntegerType))
									.withColumn("actiontype", 'actiontype.cast(IntegerType))
									.withColumn("agentinstance", 'agentinstance.cast(LongType))
									.withColumn("operation", lit("|"))
									.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("dt",lit(dateStr))
									.drop("year")
									.drop("month")
									.drop("day")
									.drop("originalschemas")

		newDf.write.mode(writeMode).saveAsTable(s"$destination.abentrylog")

		println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
	}
}

def processMobilelog(dateslist: List[LocalDate], source: String, destination: String): Unit = {
	for (dt <- dateslist){
		val dateStr = dt.toString

		println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

		val df = sql(s"select * from $source.mobilelog where year='${dt.getYear}' and month='${dt.getMonth}' and day='${dt.getDayOfMonth}'")

		val newDf = df.withColumn("userid", 'userid.cast(LongType))
									.withColumn("mailboxid", 'mailboxid.cast(LongType))
									.drop("originalschemas")
									.drop("year")
									.drop("month")
									.drop("day")
									.withColumn("operation", lit("|"))
									.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("dt",lit(dateStr))

		newDf.write.mode(writeMode).saveAsTable(s"$destination.mobilelog")

		println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
	}
}

def processClog(dateslist: List[LocalDate], source: String, destination: String): Unit = {
	for (dt <- dateslist){
		val dateStr = dt.toString

		println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

		val df = sql(s"select * from $source.clog where year='${dt.getYear}' and month='${dt.getMonth}' and day='${dt.getDayOfMonth}'")

		val newDf = df.withColumn("sessionid", 'sessionid.cast(LongType))
									.withColumn("userid", 'userid.cast(LongType))
									.withColumn("mailboxid", 'mailboxid.cast(LongType))
									.withColumn("instanceid", 'instanceid.cast(LongType))
									.withColumn("serverid", 'serverid.cast(LongType))
									.withColumn("lineno", 'lineno.cast(LongType))
									.withColumn("calltype", 'calltype.cast(IntegerType))
									.withColumn("limitid", 'limitid.cast(LongType))
									.withColumn("meteredtime", 'meteredtime.cast(DoubleType))
									.withColumn("callcost", 'callcost.cast(DoubleType))
									.withColumn("callrate", 'callrate.cast(DoubleType))
									.withColumn("callertype", 'callertype.cast(IntegerType))
									.withColumn("extracost", 'extracost.cast(DoubleType))
									.withColumn("providerid", 'providerid.cast(LongType))
									.withColumn("id_location", 'id_location.cast(LongType))
									.withColumn("messageid", 'messageid.cast(LongType))
									.withColumn("recordingid", 'recordingid.cast(LongType))
									.withColumn("actioncode", 'actioncode.cast(IntegerType))
									.withColumn("callresult", 'callresult.cast(LongType))
									.withColumn("answerno", 'answerno.cast(LongType))
									.withColumn("rejectreason", 'rejectreason.cast(IntegerType))
									.withColumn("agentid", 'agentid.cast(LongType))
									.withColumn("recipientid", 'recipientid.cast(LongType))
									.withColumn("retryno", 'retryno.cast(LongType))
									.withColumn("destinationid", 'destinationid.cast(LongType))
									.withColumn("hidden", 'hidden.cast(IntegerType))
									.drop("originalschemas")
									.withColumn("pricepercall", 'pricepercall.cast(DoubleType))
									.withColumn("startrate", 'startrate.cast(DoubleType))
									.withColumn("accesscharge", 'accesscharge.cast(DoubleType))
									.withColumn("dialingplan", 'dialingplan.cast(IntegerType))
									.withColumn("representedby", 'representedby.cast(LongType))
									.withColumn("bizlocationid", 'bizlocationid.cast(LongType))
									.drop("year")
									.drop("month")
									.drop("day")
									.withColumn("operation", lit("D"))
									.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("dt",lit(dateStr))

		newDf.write.mode(writeMode).saveAsTable(s"$destination.clog")

		println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
	}
}

def processEmaillog(dateslist: List[LocalDate], source: String, destination: String): Unit = {
	for (dt <- dateslist){
		val dateStr = dt.toString

		println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

		val df = sql(s"select * from $source.emaillog where year='${dt.getYear}' and month='${dt.getMonth}' and day='${dt.getDayOfMonth}'")

		val newDf = df.withColumn("templateid", 'templateid.cast(LongType))
									.withColumn("srvtypeid", 'srvtypeid.cast(IntegerType))
									.withColumn("userid", 'userid.cast(LongType))
									.withColumn("mailboxid", 'mailboxid.cast(LongType))
									.withColumn("brandid", 'brandid.cast(LongType))
									.withColumn("servicelevel", 'servicelevel.cast(LongType))
									.withColumn("planid", 'planid.cast(LongType))
									.withColumn("privilegeid", 'privilegeid.cast(IntegerType))
									.withColumn("testerflag", 'testerflag.cast(LongType))
									.drop("originalschemas")
									.drop("year")
									.drop("month")
									.drop("day")
									.withColumn("operation", lit("|"))
									.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("dt",lit(dateStr))

		newDf.write.mode(writeMode).saveAsTable(s"$destination.emaillog")

		println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
	}
}

def processEventslog(dateslist: List[LocalDate], source: String, destination: String): Unit = {
	for (dt <- dateslist){
		val dateStr = dt.toString

		println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

		val df = sql(s"select * from $source.eventslog where year='${dt.getYear}' and month='${dt.getMonth}' and day='${dt.getDayOfMonth}'")

		val newDf = df.withColumn("sessionid", 'sessionid.cast(LongType))
									.withColumn("duration", 'duration.cast(LongType))
									.withColumn("countcode", 'countcode.cast(LongType))
									.withColumn("mailboxid", 'mailboxid.cast(LongType))
									.withColumn("actioncode", 'actioncode.cast(IntegerType))
									.drop("originalschemas")
									.drop("year")
									.drop("month")
									.drop("day")
									.withColumn("operation", lit("|"))
									.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("dt",lit(dateStr))

		newDf.write.mode(writeMode).saveAsTable(s"$destination.eventslog")

		println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
	}
}

def processPackagesstatus(dateslist: List[LocalDate], source: String, destination: String): Unit = {
	for (dt <- dateslist){
		val dateStr = dt.toString

		println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

		val df = sql(s"select * from $source.packagesstatus where year='${dt.getYear}' and month='${dt.getMonth}' and day='${dt.getDayOfMonth}'")

		val newDf = df.withColumn("packageid", 'packageid.cast(LongType))
									.withColumn("userid", 'userid.cast(LongType))
									.withColumn("externid", 'externid.cast(LongType))
									.withColumn("blockno", 'blockno.cast(IntegerType))
									.withColumn("recordtype", 'recordtype.cast(IntegerType))
									.withColumn("amountchange", 'amountchange.cast(DoubleType))
									.withColumn("allamount", 'allamount.cast(DoubleType))
									.withColumn("limitid", 'limitid.cast(LongType))
									.withColumn("islimit", 'islimit.cast(IntegerType))
									.drop("originalschemas")
									.drop("year")
									.drop("month")
									.drop("day")
									.withColumn("operation", lit("|"))
									.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("dt",lit(dateStr))

		newDf.write.mode(writeMode).saveAsTable(s"$destination.packagesstatus")

		println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
	}
}

def processSessionlog(dateslist: List[LocalDate], source: String, destination: String): Unit = {
	for (dt <- dateslist){
		val dateStr = dt.toString

		println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

		val df = sql(s"select * from $source.sessionlog where year='${dt.getYear}' and month='${dt.getMonth}' and day='${dt.getDayOfMonth}'")

		val newDf = df.withColumn("sessionid", 'sessionid.cast(LongType))
									.withColumn("ipaddress", 'ipaddress.cast(LongType))
									.withColumn("mailboxid", 'mailboxid.cast(LongType))
									.withColumn("userid", 'userid.cast(LongType))
									.withColumn("serverid", 'serverid.cast(LongType))
									.drop("originalschemas")
									.withColumn("adminid", 'adminid.cast(LongType))
									.drop("year")
									.drop("month")
									.drop("day")
									.withColumn("operation", lit("|"))
									.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("dt",lit(dateStr))

		newDf.write.mode(writeMode).saveAsTable(s"$destination.sessionlog")

		println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
	}
}

def processSipagent(dateslist: List[LocalDate], source: String, destination: String): Unit = {
	for (dt <- dateslist){
		val dateStr = dt.toString

		println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

		val df = sql(s"select * from $source.sipagent where year='${dt.getYear}' and month='${dt.getMonth}' and day='${dt.getDayOfMonth}'")

		val newDf = df.withColumn("userid", 'userid.cast(LongType))
									.withColumn("mailboxid", 'mailboxid.cast(LongType))
									.withColumn("instanceid", 'instanceid.cast(LongType))
									.withColumn("version", 'version.cast(LongType))
									.withColumn("edition", 'edition.cast(LongType))
									.withColumn("skintype", 'skintype.cast(LongType))
									.withColumn("skinschema", 'skinschema.cast(LongType))
									.drop("originalschemas")
									.drop("year")
									.drop("month")
									.drop("day")
									.withColumn("operation", lit("|"))
									.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
									.withColumn("dt",lit(dateStr))

		newDf.write.mode(writeMode).saveAsTable(s"$destination.sipagentlog")

		println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
	}
}

val datesList = startDate.toEpochDay.until(endDate.toEpochDay).map(LocalDate.ofEpochDay).toList

tableName match {
	case "abentrylog" => processAbentrylog(datesList, sourseDB, destinationDB)
	case "mobilelog" => processMobilelog(datesList, sourseDB, destinationDB)
	case "clog" => processClog(datesList, sourseDB, destinationDB)
	case "emaillog" => processEmaillog(datesList, sourseDB, destinationDB)
	case "eventslog" => processEventslog(datesList, sourseDB, destinationDB)
	case "packagesstatus" => processPackagesstatus(datesList, sourseDB, destinationDB)
	case "sessionlog" => processSessionlog(datesList, sourseDB, destinationDB)
	case "sipagent" => processSipagent(datesList, sourseDB, destinationDB)
	case _ => {
		println(s"${LocalDateTime.now.toString} | Unsupported table: $tableName")
		System.exit(0)
	}
}

println(s"${LocalDateTime.now.toString} | Finished")

System.exit(0)