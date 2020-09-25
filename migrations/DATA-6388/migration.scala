    import java.time.LocalDate

    import org.apache.hadoop.fs.FileSystem
    import org.apache.hadoop.fs.Path
    import org.apache.spark.deploy.SparkHadoopUtil
    import org.apache.spark.sql.DataFrame
    import org.apache.spark.sql.functions.col
    import org.apache.spark.sql.SaveMode

    val fieldIsNull = "name"

    val args = spark.sqlContext.getConf("spark.driver.args").split(",")
    val latestDate = args(0)
    val oldestDate = args(1)
    val sourceTable = args(2)
    val tmpDir = args(3)

    implicit val fs: FileSystem = FileSystem.get(SparkHadoopUtil.get.conf)

    def createDateList(): List[String] = {
        val first = LocalDate.parse(latestDate)
        val last = LocalDate.parse(oldestDate)
        if (last.equals(first)) return List.apply(latestDate)
        last.toEpochDay.to(first.toEpochDay).
            toList.sorted.reverse.
            map(date => LocalDate.ofEpochDay(date).toString)
    }

    def processDt(sourcePartitionLocation: String, tmpPartitionLocation: String, dt: String): Unit = {
        val df = spark.read.option("header", "true").format("parquet").load(sourcePartitionLocation)
        val nullValues = df.filter(col(fieldIsNull).isNull).count()
        if (nullValues > 0) {
            removeNullValues(df, sourcePartitionLocation, tmpPartitionLocation)
            println("PARTITION UPDATED: " + dt)
        } else println("PARTITION HAS NO NULL VALUES: " + dt)
    }

    def removeNullValues(df: DataFrame, sourcePartitionLocation: String, tmpPartitionLocation: String): Unit = {
        df.filter(col(fieldIsNull).isNotNull).write.mode(SaveMode.Overwrite).option("header", "true").parquet(tmpPartitionLocation)
        fs.delete(new Path(sourcePartitionLocation), true)
        fs.rename(new Path(tmpPartitionLocation), new Path(sourceTable))
    }

    val dates: List[String] = createDateList()

    fs.mkdirs(new Path(tmpDir))

    for (dt <- dates) {
        val sourcePartitionLocation = sourceTable + "dt=" + dt
        val tmpPartitionLocation = tmpDir + "dt=" + dt

        if (fs.exists(new Path(sourcePartitionLocation))) {
            processDt(sourcePartitionLocation, tmpPartitionLocation, dt)
        } else println("PARTITION HAS NOT BEEN FOUND: " + dt)
    }

    fs.delete(new Path(tmpDir), true)

    System.exit(0)
