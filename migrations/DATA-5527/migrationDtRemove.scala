import scala.util.matching.Regex
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col
import scala.io.Source
import java.time.{LocalDate, LocalDateTime}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructType

spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

val args = spark.sqlContext.getConf("spark.driver.args").split(",")

val rootSourcePath = new Path(args(0))
val rootTmpPath = new Path(args(1))
val rootDestinationPath = new Path(args(2))

val deleteSource = args(3).toBoolean

val startdate = LocalDate.parse(args(4))
val enddate = LocalDate.parse(args(5))

val fs = FileSystem.get(sc.hadoopConfiguration)
val utils = new Utils(fs)

val partitionList = utils.getPartitionList(rootSourcePath)

if (partitionList.isEmpty) {
    utils.logInfo("No partitions suitable for the time conditions")
    System.exit(0)
}

def migrate(df: DataFrame, schema: StructType): DataFrame = {
    df.drop("dt")
}

for (i <- partitionList) {
    utils.logInfo(s"Do partition: $i")

    // Prepare temporary location
    utils.prepateTmpLocation(rootTmpPath, i)

    // Read data
    utils.logInfo("Processing data")
    val partitionPath = utils.getPartitionPath(rootSourcePath, i)
    val df = spark.read.parquet(partitionPath.toString)
    val schema = df.schema

    //Migrate data
    val migratedDf = migrate(df, schema)

    //Write data to temporary location
    utils.writeDfToTmp(migratedDf, rootTmpPath, i)

    if (deleteSource) {
        // Delete source location
        utils.deleteSourceData(partitionPath, i)

        // Rename tmp to source back
        val partitionDestinationPath = utils.getPartitionPath(rootDestinationPath, i)
        utils.moveData(rootTmpPath, partitionDestinationPath, i)
    }
}

utils.logInfo("Finished")

System.exit(0)

class Utils(fs: FileSystem) {

    private val pattern = new Regex("(?!dt=)(\\d+-\\d+-\\d+)")

    def getPartitionList(sourcePath: Path): List[LocalDate] = {
        fs.listStatus(sourcePath)
            .toList
            .map(_.getPath.toString)
            .map(item => pattern findFirstIn item)
            .map(_.get)
            .map(LocalDate.parse(_))
            .filter(_.isAfter(startdate))
            .filter(_.isBefore(enddate))
    }

    def writeDfToTmp(df: DataFrame, rootTmpPath: Path, date: LocalDate) = {
        val partitionTmpPathStr = getPartitionPathStr(rootTmpPath, date)
        if (!df.rdd.isEmpty) {
            df.coalesce(20)
                .write
                .mode(SaveMode.Overwrite)
                .parquet(partitionTmpPathStr)
        }
    }

    def prepateTmpLocation(rootTmpPath: Path, date: LocalDate) = {
        val partitionTmpPath = getPartitionPath(rootTmpPath, date)
        if (fs.exists(partitionTmpPath)) {
            logInfo(s"Temporary directory $partitionTmpPath exists, deleting")
            if (fs.delete(partitionTmpPath, true)) {
                logInfo(s"Temporary location $partitionTmpPath was successfully deleted")
            } else {
                throw new Exception(s"Didn`t delete tmp partition $date")
            }
        }
        println(s"${LocalDateTime.now.toString} | Create temporary directory $partitionTmpPath")
        fs.mkdirs(partitionTmpPath)
    }

    def deleteSourceData(path: Path, date: LocalDate) = {
        logInfo("Deleting source location")
        if (fs.delete(path, true)) {
            logInfo(s"Source location $path was successfully deleted")
        } else {
            throw new Exception(s"Didn`t delete partition $date")
        }
    }

    def moveData(rootTmpPath: Path, partitionPath: Path, date: LocalDate) = {
        val partitionTmpPath = getPartitionPath(rootTmpPath, date)
        logInfo("Moving data")
        if (fs.rename(partitionTmpPath, partitionPath)) {
            logInfo(s"Partition $date was successfully copied to from tmp to target location")
        } else {
            throw new Exception(s"Error copying partition from temp location $partitionTmpPath to target $partitionPath")
        }
    }

    def getPartitionPath(path: Path, date: LocalDate): Path = {
        new Path(getPartitionPathStr(path, date))
    }

    private def getPartitionPathStr(path: Path, date: LocalDate): String = {
        s"${path.toString}/dt=$date"
    }

    def logInfo(msg: String): Unit = {
        println(s"${LocalDateTime.now.toString} | $msg")
    }
}