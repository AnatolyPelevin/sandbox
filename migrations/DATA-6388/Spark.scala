    import java.time.LocalDate

    import org.apache.hadoop.fs.FileSystem
    import org.apache.hadoop.fs.FileUtil
    import org.apache.hadoop.fs.Path
    import org.apache.spark.deploy.SparkHadoopUtil
    import org.apache.spark.sql.DataFrame
    import org.apache.spark.sql.functions.col
    import org.apache.spark.sql.SaveMode

    val sourceTable = "hdfs://nameservice1/envs/production/SFDC/out-data/SFDC_ETL_postgres/dfr_pivot_w_formulas/"
    val tmpDir = "hdfs://nameservice1/envs/production/SFDC/tmp/dfr_pivot_w_formulas/"
    val fieldIsNull = "name"

    val args = spark.sqlContext.getConf("spark.driver.args").split(",")
    val latestDate = args(0)
    val oldestDate = args(1)

    implicit val fs: FileSystem = FileSystem.get(SparkHadoopUtil.get.conf)

    def createDateList(): List[String] = {
        var first = LocalDate.parse(latestDate)
        var last = LocalDate.parse(oldestDate)
        if (last.isAfter(first)) {
            last = LocalDate.parse(latestDate)
            first = LocalDate.parse(oldestDate)
        }
        if (last.equals(first)) return List.apply(latestDate)

        val dates = scala.collection.mutable.Map[String, Long]()
        for (date <- last.toEpochDay to first.toEpochDay) {
            dates += date.toString -> date
        }
        dates.values.toList.sorted.reverse.map(date => LocalDate.ofEpochDay(date).toString)
    }

    def processDt(sourceDt: String, tmpDt: String, dt: String): Unit = {
        val df = spark.read.option("header", "true").format("parquet").load(sourceDt)
        val nullValues = df.filter(col(fieldIsNull).isNull).count()
        if (nullValues > 0) {
            removeNullValues(df, sourceDt, tmpDt)
            println("PARTITION UPDATED: " + dt)
        } else println("PARTITION HAS NO NULL VALUES: " + dt)
    }

    def removeNullValues(df: DataFrame, oldDt: String, newDt: String): Unit = {
        df.filter(col(fieldIsNull).isNotNull).write.mode(SaveMode.Overwrite).option("header", "true").parquet(newDt)
        fs.delete(new Path(oldDt), true)
        FileUtil.copy(fs, new Path(newDt), fs, new Path(sourceTable), true, true, SparkHadoopUtil.get.conf)
    }

    val dates: List[String] = createDateList()

    fs.mkdirs(new Path(tmpDir))

    for (dt <- dates) {
        val sourceDt = sourceTable + "dt=" + dt
        val tmpDt = tmpDir + "dt=" + dt

        if (fs.exists(new Path(sourceDt))) {
            processDt(sourceDt, tmpDt, dt)
        } else println("PARTITION HAS NOT BEEN FOUND: " + dt)
    }

    fs.delete(new Path(tmpDir), true)

    System.exit(0)
