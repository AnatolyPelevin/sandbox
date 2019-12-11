import scala.util.matching.Regex
import org.apache.hadoop.fs._
import scala.io.Source
import java.time.LocalDate

spark.conf.set("spark.sql.avro.compression.codec","snappy")
spark.conf.set("spark.executor.extraJavaOptions","-XX:MaxDirectMemorySize=2048M")
spark.conf.set("spark.executor.memoryOverhead","1536")
spark.conf.set("spark.executor.memory","1G")
spark.conf.set("spark.dynamicAllocation.maxExecutors","10")
spark.conf.set("spark.dynamicAllocation.enabled","true")
spark.conf.set("spark.dynamicAllocation.initialExecutors","10")
spark.conf.set("spark.dynamicAllocation.minExecutors","10")


val args = spark.sqlContext.getConf("spark.driver.args").split(",")
val sourceStr = args(0)
val destinationStr = args(1)
val tmpStr = args(2)
val startDate = args(3).split("-") match {case Array(x,y,z) => (x.toInt, y.toInt,z.toInt)}
val endDate = args(4).split("-") match {case Array(x,y,z) => (x.toInt, y.toInt,z.toInt)}

val start = LocalDate.of(startDate._1,startDate._2,startDate._3)
val end = LocalDate.of(endDate._1,endDate._2,endDate._3)

val pattern = new Regex("(?!dt=)(\\d+-\\d+-\\d+)")
val source = new Path(sourceStr)

val fs = FileSystem.get( sc.hadoopConfiguration )
val list = fs.listStatus(source).toList.map(_.getPath.toString).map(item => pattern findFirstIn item).filter(_.nonEmpty).map(_.get)

val datesList = list.map(_.split("-") match {case Array(x,y,z) => (x.toInt, y.toInt,z.toInt)}).map(item => LocalDate.of(item._1, item._2, item._3))
val partitionList = datesList.filter(_.isAfter(start)).filter(_.isBefore(end))

if (partitionList.length == 0) {
	println("Have not dates to process")
}
for (i <- partitionList) {
        println(s"Do partition: $i")

        if (!fs.exists(new Path(s"$destinationStr/tmp/dt=$i"))) {
        	val av = spark.read.format("avro").load(s"$sourceStr/dt=$i").write.format("avro").save(s"$tmpStr/dt=$i")
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
}
println("Finished")
System.exit(0)