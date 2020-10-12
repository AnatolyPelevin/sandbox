import java.io.File
import org.apache.spark.sql._
import spark.sql
import java.time.{LocalDate, LocalDateTime}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.hadoop.fs.{FileSystem, Path}

val warehouseLocation = new File("spark-warehouse").getAbsolutePath
val spark = SparkSession.builder().appName("CLP to HDG data migration").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()

val args = spark.sqlContext.getConf("spark.driver.args").split(",")
val sourseDB = args(0)
val destinationDB= args(1)
val startDate = LocalDate.parse(args(2))
val endDate = LocalDate.parse(args(3))
val tableName = args(4)
val hdfsSourcePath="/envs/production/CLP/out-data/CLP_ETL-coordinator"
val hdfsFolder = s"envs/production/HDG/out-data"
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

def processAbentrylog(dateslist: List[LocalDate], source: String, destination: String): Unit = {

	println("start migration abentrylog")
  val tableFolder = s"$hdfsFolder/abentrylog"

  for (dt <- dateslist) {
		val dateStr = dt.toString
		val hdfsPath = new Path(s"${hdfsSourcePath}/abentrylog/year=${dt.getYear}/month=${dt.getMonthValue}/day=${dt.getDayOfMonth}")
		if (fs.exists(hdfsPath)) {
			val content = fs.listStatus(hdfsPath).filter(_.isDirectory).map(_.getPath)

			val df = sql(s"select * from $source.abentrylog where year='${dt.getYear}' and month='${dt.getMonthValue}' and day='${dt.getDayOfMonth}'")

			if (content.size > 0 && !df.isEmpty) {

				println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

				val newDf = df.withColumn("abentryid", 'abentryid.cast(LongType))
					.withColumn("fmcounter", 'fmcounter.cast(LongType))
					.withColumn("mailboxid", 'mailboxid.cast(LongType))
					.withColumn("entrytype", 'entrytype.cast(IntegerType))
					.withColumn("objecttype", 'objecttype.cast(IntegerType))
					.withColumn("actiontype", 'actiontype.cast(IntegerType))
					.withColumn("agentinstance", 'agentinstance.cast(LongType))
					.withColumn("operation", lit("I"))
					.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("dt", lit(dateStr))
					.drop("year")
					.drop("month")
					.drop("day")
					.drop("originalschemas")

				val partitionFolder = s"/$tableFolder/dt=$dateStr"
				newDf.write.mode(SaveMode.Overwrite).parquet(partitionFolder)

				println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
			}
			else {
				println(s"${LocalDateTime.now.toString} | partition: $dateStr is empty")
			}
		} else {
			println(s"$hdfsPath does't exists")
		}
  }
	println("migration of abentrylog is done")
	spark.sql(s"msck repair table $destinationDB.abentrylog")
}

def processMobilelog(dateslist: List[LocalDate], source: String, destination: String): Unit = {

	println("start migration mobilelog")
	val tableFolder = s"$hdfsFolder/mobilelog"

	for (dt <- dateslist){
		val dateStr = dt.toString
		val hdfsPath = new Path(s"${hdfsSourcePath}/mobilelog/year=${dt.getYear}/month=${dt.getMonthValue}/day=${dt.getDayOfMonth}")
		if (fs.exists(hdfsPath)) {
			val content = fs.listStatus(hdfsPath).filter(_.isDirectory).map(_.getPath)

			val df = sql(s"select * from $source.mobilelog where year='${dt.getYear}' and month='${dt.getMonthValue}' and day='${dt.getDayOfMonth}'")

			if (content.size > 0 && !df.isEmpty) {

				println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

				val newDf = df.withColumn("userid", 'userid.cast(LongType))
					.withColumn("mailboxid", 'mailboxid.cast(LongType))
					.drop("originalschemas")
					.drop("year")
					.drop("month")
					.drop("day")
					.withColumn("operation", lit("I"))
					.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("dt", lit(dateStr))

				val partitionFolder = s"/$tableFolder/dt=$dateStr"
				newDf.write.mode(SaveMode.Overwrite).parquet(partitionFolder)

				println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
			}
			else {
				println(s"${LocalDateTime.now.toString} | partition: $dateStr is empty")
			}
		} else {
			println(s"$hdfsPath does't exists")
		}
	}
	println("migration of mobilelog is done")
	spark.sql(s"msck repair table $destinationDB.mobilelog")
}

def processClog(dateslist: List[LocalDate], source: String, destination: String): Unit = {

	println("start migration clog")
	val tableFolder = s"$hdfsFolder/clog"

	for (dt <- dateslist){
		val dateStr = dt.toString
		val hdfsPath = new Path(s"${hdfsSourcePath}/clog/year=${dt.getYear}/month=${dt.getMonthValue}/day=${dt.getDayOfMonth}")
		if (fs.exists(hdfsPath)) {
			val content = fs.listStatus(hdfsPath).filter(_.isDirectory).map(_.getPath)

			val df = sql(s"select * from $source.clog where year='${dt.getYear}' and month='${dt.getMonthValue}' and day='${dt.getDayOfMonth}'")

			if (content.size > 0 && !df.isEmpty) {

				println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

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
					.withColumn("operation", lit("I"))
					.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("dt", lit(dateStr))

				val partitionFolder = s"/$tableFolder/dt=$dateStr"
				newDf.write.mode(SaveMode.Overwrite).parquet(partitionFolder)

				println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
			}
			else {
				println(s"${LocalDateTime.now.toString} | partition: $dateStr is empty")
			}
		} else {
			println(s"$hdfsPath does't exists")
		}
	}
	println("migration of clog is done")
	spark.sql(s"msck repair table $destinationDB.clog")

}

def processEmaillog(dateslist: List[LocalDate], source: String, destination: String): Unit = {

	println("start migration emaillog")
	val tableFolder = s"$hdfsFolder/emaillog"

	for (dt <- dateslist){
		val dateStr = dt.toString
		val hdfsPath = new Path(s"${hdfsSourcePath}/emaillog/year=${dt.getYear}/month=${dt.getMonthValue}/day=${dt.getDayOfMonth}")
		if (fs.exists(hdfsPath)) {
			val content = fs.listStatus(hdfsPath).filter(_.isDirectory).map(_.getPath)

			println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

			val df = sql(s"select * from $source.emaillog where year='${dt.getYear}' and month='${dt.getMonthValue}' and day='${dt.getDayOfMonth}'")

			if (content.size > 0 && !df.isEmpty) {

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
					.withColumn("operation", lit("I"))
					.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("dt", lit(dateStr))

				val partitionFolder = s"/$tableFolder/dt=$dateStr"
				newDf.write.mode(SaveMode.Overwrite).parquet(partitionFolder)

				println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
			}
			else {
				println(s"${LocalDateTime.now.toString} | partition: $dateStr is empty")
			}
		} else {
			println(s"$hdfsPath does't exists")
		}
	}
	println("migration of emaillog is done")
	spark.sql(s"msck repair table $destinationDB.emaillog")
}

def processEventslog(dateslist: List[LocalDate], source: String, destination: String): Unit = {

	println("start migration eventslog")
	val tableFolder = s"$hdfsFolder/eventslog"

	for (dt <- dateslist){
		val dateStr = dt.toString
		val hdfsPath = new Path(s"${hdfsSourcePath}/eventslog/year=${dt.getYear}/month=${dt.getMonthValue}/day=${dt.getDayOfMonth}")
		if (fs.exists(hdfsPath)) {
			val content = fs.listStatus(hdfsPath).filter(_.isDirectory).map(_.getPath)

			val df = sql(s"select * from $source.eventslog where year='${dt.getYear}' and month='${dt.getMonthValue}' and day='${dt.getDayOfMonth}'")

			if (content.size > 0 && !df.isEmpty) {

				println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

				val newDf = df.withColumn("sessionid", 'sessionid.cast(LongType))
					.withColumn("duration", 'duration.cast(LongType))
					.withColumn("countcode", 'countcode.cast(LongType))
					.withColumn("mailboxid", 'mailboxid.cast(LongType))
					.withColumn("actioncode", 'actioncode.cast(IntegerType))
					.drop("originalschemas")
					.drop("year")
					.drop("month")
					.drop("day")
					.withColumn("operation", lit("I"))
					.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("dt", lit(dateStr))

				val partitionFolder = s"/$tableFolder/dt=$dateStr"
				newDf.write.mode(SaveMode.Overwrite).parquet(partitionFolder)
				println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
			}
			else {
				println(s"${LocalDateTime.now.toString} | partition: $dateStr is empty")
			}
		} else {
			println(s"$hdfsPath does't exists")
		}
	}
	println("migration of eventslog is done")
	spark.sql(s"msck repair table $destinationDB.eventslog")
}

def processPackagesstatus(dateslist: List[LocalDate], source: String, destination: String): Unit = {

	println("start migration packagesstatus")
	val tableFolder = s"$hdfsFolder/packagesstatus"

	for (dt <- dateslist){
		val dateStr = dt.toString
		val hdfsPath = new Path(s"${hdfsSourcePath}/packagesstatus/year=${dt.getYear}/month=${dt.getMonthValue}/day=${dt.getDayOfMonth}")
		if (fs.exists(hdfsPath)) {
			val content = fs.listStatus(hdfsPath).filter(_.isDirectory).map(_.getPath)

			val df = sql(s"select * from $source.packagesstatus where year='${dt.getYear}' and month='${dt.getMonthValue}' and day='${dt.getDayOfMonth}'")

			if (content.size > 0 && !df.isEmpty) {

				println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

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
					.withColumn("operation", lit("I"))
					.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("dt", lit(dateStr))

				val partitionFolder = s"/$tableFolder/dt=$dateStr"
				newDf.write.mode(SaveMode.Overwrite).parquet(partitionFolder)

				println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
			}
			else {
				println(s"${LocalDateTime.now.toString} | partition: $dateStr is empty")
			}
		} else {
			println(s"$hdfsPath does't exists")
		}
	}
	println("migration of packagesstatus is done")
	spark.sql(s"msck repair table $destinationDB.packagesstatus")
}

def processSessionlog(dateslist: List[LocalDate], source: String, destination: String): Unit = {

	println("start migration sessionlog")
	val tableFolder = s"$hdfsFolder/sessionlog"

	for (dt <- dateslist) {
		val dateStr = dt.toString
		val hdfsPath = new Path(s"${hdfsSourcePath}/sessionlog/year=${dt.getYear}/month=${dt.getMonthValue}/day=${dt.getDayOfMonth}")
		if (fs.exists(hdfsPath)) {
			val content = fs.listStatus(hdfsPath).filter(_.isDirectory).map(_.getPath)

			val df = sql(s"select * from $source.sessionlog where year='${dt.getYear}' and month='${dt.getMonthValue}' and day='${dt.getDayOfMonth}'")

			if (content.size > 0 && !df.isEmpty) {

				println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

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
					.withColumn("operation", lit("I"))
					.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("dt", lit(dateStr))

				val partitionFolder = s"/$tableFolder/dt=$dateStr"
				newDf.write.mode(SaveMode.Overwrite).parquet(partitionFolder)

				println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
			}
			else {
				println(s"${LocalDateTime.now.toString} | partition: $dateStr is empty")
			}
		} else {
			println(s"$hdfsPath does't exists")
		}
	}
	println("migration of sessionlog is done")
	spark.sql(s"msck repair table $destinationDB.sessionlog")
}

def processSipagent(dateslist: List[LocalDate], source: String, destination: String): Unit = {

	println("start migration sipagentlog")
	val tableFolder = s"$hdfsFolder/sipagentlog"

	for (dt <- dateslist) {
		val dateStr = dt.toString
		val hdfsPath = new Path(s"${hdfsSourcePath}/sipagent/year=${dt.getYear}/month=${dt.getMonthValue}/day=${dt.getDayOfMonth}")
		if (fs.exists(hdfsPath)) {
			val content = fs.listStatus(hdfsPath).filter(_.isDirectory).map(_.getPath)

			val df = sql(s"select * from $source.sipagent where year='${dt.getYear}' and month='${dt.getMonthValue}' and day='${dt.getDayOfMonth}'")

			if (content.size > 0 && !df.isEmpty) {

				println(s"${LocalDateTime.now.toString} | Do partition: $dateStr")

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
					.withColumn("operation", lit("I"))
					.withColumn("ts_inserted_to_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("ts_read_from_kafka", lit(dateStr).cast("Timestamp"))
					.withColumn("dt", lit(dateStr))

				val partitionFolder = s"/$tableFolder/dt=$dateStr"
				newDf.write.mode(SaveMode.Overwrite).parquet(partitionFolder)

				println(s"${LocalDateTime.now.toString} | Partition $dateStr was processed")
			}
			else {
				println(s"${LocalDateTime.now.toString} | partition: $dateStr is empty")
			}
		} else {
			println(s"$hdfsPath does't exists")
		}
	}
	println("migration of sipagentlog is done")
	spark.sql(s"msck repair table $destinationDB.sipagentlog")
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