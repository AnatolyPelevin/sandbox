import org.apache.hadoop.fs._
import scala.io.Source
import java.io.{FileNotFoundException, IOException}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

spark.conf.set("spark.sql.avro.compression.codec","snappy")
spark.conf.set("spark.sql.parquet.compression.codec","snappy")
spark.conf.set("spark.executor.extraJavaOptions","-XX:MaxDirectMemorySize=1024M")
spark.conf.set("spark.executor.memoryOverhead","1536")
spark.conf.set("spark.executor.memory","1G")
spark.conf.set("spark.dynamicAllocation.maxExecutors","10")

val fullDatePattern = DateTimeFormatter.ofPattern("yyyy-MM-dd")
val pattern = "(\\d{4}-\\d{2}-\\d{2})".r

val args = spark.sqlContext.getConf("spark.driver.args").split(",")
val sourceStr = args(0)
val destinationStr = args(1)
val tmpStr = args(2)
val start = LocalDate.parse(args(3), fullDatePattern)
val end = LocalDate.parse(args(4), fullDatePattern)
val dataType = args(5)

val source = new Path(sourceStr)

val fs = FileSystem.get( sc.hadoopConfiguration )

val list = fs.listStatus(source).toList.map(_.getPath.toString).map(item => pattern findFirstIn item).filter(_.nonEmpty).map(_.get)

val partitionList = list.map(LocalDate.parse(_, fullDatePattern)).filter(_.isAfter(start)).filter(_.isBefore(end))
println(partitionList)

if (partitionList.length == 0) {
        println("Have not dates to process")
}

for (i <- partitionList) {

    println(s"Do partition: $i")
    try {
        if (!fs.exists(new Path(s"$destinationStr/tmp/dt=$i"))) {
            if (dataType == "textfile") {
                spark.read.text(s"$sourceStr/dt=$i").coalesce(30).write.option("compression","snappy").text(s"$tmpStr/dt=$i")
            } else {
                spark.read.format(dataType).load(s"$sourceStr/dt=$i").write.format(dataType).save(s"$tmpStr/dt=$i")
            }
        } else {
            throw new Exception("Temporary folder have file with the same name")
        }

        println(s"Compressed partition $i was successfully write to tmp directory")

        val destinationWithDir = new Path(s"$destinationStr/dt=$i")

        if (fs.exists(destinationWithDir)) {
            if (fs.delete(destinationWithDir,true)) {
                println(s"Uncompressed partition $i was successfully deleted")
            } else {
                throw new Exception(s"Didn`t delete partition $i")
            }
        }
        if (fs.rename(new Path(s"$tmpStr/dt=$i"),new Path(s"$destinationStr/dt=$i"))) {
            println(s"Compressed partition $i was successfully moved to the destination folder")
        } else {
            throw new Exception(s"Didn`t move compressed partition $i to the destination folder")
        }

        println(s"Partition $i was processed")
    } catch {
        case e: FileNotFoundException => println("FileNotFoundException was catched")
        case e: IOException => println("IOException was catched")
        case _: Throwable => println("Got unsuported exception") 
    }
}

println("Finished")

System.exit(0)