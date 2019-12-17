import org.apache.hadoop.fs._
import scala.io.Source
import java.io.{FileNotFoundException, IOException}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

spark.conf.set("spark.sql.avro.compression.codec", "snappy")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

val fullDatePattern = DateTimeFormatter.ofPattern("yyyy-MM-dd")
val pattern = "(\\d{4}-\\d{2}-\\d{2})".r

val args = spark.sqlContext.getConf("spark.driver.args").split(",")
val sourceStr = args(0)
val destinationStr = args(1)
val tmpStr = args(2)
val start = LocalDate.parse(args(3), fullDatePattern)
val end = LocalDate.parse(args(4), fullDatePattern)
val dataType = args(5)
val splitFactor = args(6).toInt

val source = new Path(sourceStr)

val fs = FileSystem.get(sc.hadoopConfiguration)

val list = fs.listStatus(source).toList.map(_.getPath.toString).map(item => pattern findFirstIn item).filter(_.nonEmpty).map(_.get)

val partitionList = list.map(LocalDate.parse(_, fullDatePattern)).filter(_.isAfter(start)).filter(_.isBefore(end))

if (partitionList.length == 0) {
    println(s"${LocalDateTime.now.toString} | Have not dates to process")
}

for (i <- partitionList) {

    println(s"${LocalDateTime.now.toString} | Do partition: $i")
    try {
        if (!fs.exists(new Path(s"$destinationStr/tmp/dt=$i"))) {
            dataType match {
                case "textfile" => spark.read.text(s"$sourceStr/dt=$i").coalesce(splitFactor).write.option("compression", "snappy").text(s"$tmpStr/dt=$i")
                case "parquet" | "avro" => spark.read.format(dataType).load(s"$sourceStr/dt=$i").write.format(dataType).save(s"$tmpStr/dt=$i")
                case _ => {
                    println(s"${LocalDateTime.now.toString} | Unsupported data type")
                    System.exit(0)
                }
            }
        } else {
            throw new Exception("Temporary folder have file with the same name")
        }

        println(s"${LocalDateTime.now.toString} | Compressed partition $i was successfully write to tmp directory")

        val destinationWithDir = new Path(s"$destinationStr/dt=$i")

        if (fs.exists(destinationWithDir)) {
            if (fs.delete(destinationWithDir, true)) {
                println(s"${LocalDateTime.now.toString} | Uncompressed partition $i was successfully deleted")
            } else {
                throw new Exception(s"Didn`t delete partition $i")
            }
        }
        if (fs.rename(new Path(s"$tmpStr/dt=$i"), new Path(s"$destinationStr/dt=$i"))) {
            println(s"${LocalDateTime.now.toString} | Compressed partition $i was successfully moved to the destination folder")
        } else {
            throw new Exception(s"Didn`t move compressed partition $i to the destination folder")
        }

        println(s"${LocalDateTime.now.toString} | Partition $i was processed")
    } catch {
        case e: FileNotFoundException =>
            println(s"${LocalDateTime.now.toString} | FileNotFoundException was catched")
            e.printStackTrace()
        case e: IOException =>
            println(s"${LocalDateTime.now.toString} | IOException was catched")
            e.printStackTrace()
        case e: Exception =>
            println(s"${LocalDateTime.now.toString} | Exception was catched")
            e.printStackTrace()
    }
}

println(s"${LocalDateTime.now.toString} | Finished")

System.exit(0)