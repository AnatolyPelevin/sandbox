package com.ringcentral.analytics.etl.progress

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.stream.Collectors.toList

import com.ringcentral.analytics.etl.json.JsonStore

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable


class ProgressLogger private(queue: ConcurrentLinkedQueue[MigrationStep], jsonHelper: JsonStore) {

    def log(migrationItem: MigrationStep): Unit = {
        queue.add(migrationItem)
    }

    def flush(): Unit = {
        val result: mutable.Seq[MigrationStep] = asScalaBuffer(queue.stream().collect(toList()))
        jsonHelper.write(result)
        println(result)
    }
}

object ProgressLogger {
    def apply(helper: JsonStore): ProgressLogger = {
        new ProgressLogger(new ConcurrentLinkedQueue[MigrationStep], helper)
    }
}


