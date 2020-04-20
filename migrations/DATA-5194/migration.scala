import scala.util.matching.Regex
import org.apache.hadoop.fs._
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

val pattern = new Regex("(?!dt=)(\\d+-\\d+-\\d+)")

val fs = FileSystem.get(sc.hadoopConfiguration)
val partitionList = fs.listStatus(rootSourcePath)
  .toList
  .map(_.getPath.toString)
  .map(item => pattern findFirstIn item)
  .map(_.get)
  .map(LocalDate.parse(_))
  .filter(_.isAfter(startdate))
  .filter(_.isBefore(enddate))

if (partitionList.isEmpty) {
  println(s"${LocalDateTime.now.toString} | No new work for today")
}

for (i <- partitionList) {
  println(s"${LocalDateTime.now.toString} | Do partition: $i")

  // Prepare temporary location
  prepateTmpLocation(rootTmpPath, i)

  // Read data
  val partitionPath = getPartitionPath(rootSourcePath,i)
  println(s"${LocalDateTime.now.toString} | Processing data")
  val df = spark.read.parquet(partitionPath.toString)
  val schema = df.schema

  //Migrate data
  val newDf = migrateDF(df)

  //Write data to temporary location
  writeDFToTmpLocation(newDf,schema,rootTmpPath,i)

  // Delete source location
  if (deleteSource) {
    deleteSourceLocation(partitionPath, i)
  }

  // Rename tmp to source back
  val partitionDestinationPath = getPartitionPath(rootDestinationPath,i)
  moveData(rootTmpPath, partitionDestinationPath, i)
}

def migrateDF(df: DataFrame): DataFrame = {
  df.withColumn("tmu_Tmp", df.apply("tmu").cast(LongType)).drop("tmu").withColumnRenamed("tmu_Tmp", "tmu")
}

def writeDFToTmpLocation(df: DataFrame, schema: StructType, rootTmpPath: Path, date: LocalDate) = {
  val partitionTmpPathStr = getPartitionPathStr(rootTmpPath, date)
  if (!df.rdd.isEmpty) {
    df.select(schema.fields.map(_.name).map(col): _*)
      .coalesce(20)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(partitionTmpPathStr)
  }
}

def prepateTmpLocation(rootTmpPath: Path, date: LocalDate) = {
  val partitionTmpPath = getPartitionPath(rootTmpPath, date)
  if (fs.exists(partitionTmpPath)) {
    println(s"${LocalDateTime.now.toString} | Temporary directory $partitionTmpPath exists, deleting")
    if (fs.delete(partitionTmpPath, true)) {
      println(s"${LocalDateTime.now.toString} | Temporary location $partitionTmpPath was successfully deleted")
    } else {
      throw new Exception(s"Didn`t delete partition $date")
    }
  }
  println(s"${LocalDateTime.now.toString} | Create temporary directory $partitionTmpPath")
  fs.mkdirs(partitionTmpPath)
}

def deleteSourceLocation(path: Path, date: LocalDate) = {
  println(s"${LocalDateTime.now.toString} | Deleting source location")
  if (fs.delete(path, true)) {
    println(s"${LocalDateTime.now.toString} | Source location $path was successfully deleted.")
  } else {
    throw new Exception(s"Didn`t delete partition $date")
  }
}

def moveData(rootTmpPath: Path, partitionPath: Path, date: LocalDate) = {
  val partitionTmpPath = getPartitionPath(rootTmpPath, date)
  println(s"${LocalDateTime.now.toString} | Moving data")
  if (fs.rename(partitionTmpPath, partitionPath)) {
    println(s"${LocalDateTime.now.toString} | Partition $date was successfully copied to from tmp to target location")
  } else {
    throw new Exception(s"Error copying partition from temp location $partitionTmpPath to target $partitionPath")
  }
}

def getPartitionPath(path: Path, date: LocalDate): Path = {
  new Path(getPartitionPathStr(path,date))
}

def getPartitionPathStr(path: Path, date: LocalDate): String = {
  s"${path.toString}/dt=$date"
}

println(s"${LocalDateTime.now.toString} | Finished")

System.exit(0)