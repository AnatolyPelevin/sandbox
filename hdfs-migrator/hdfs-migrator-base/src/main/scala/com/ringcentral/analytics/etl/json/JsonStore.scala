package com.ringcentral.analytics.etl.json

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.ringcentral.analytics.etl.progress.MigrationStep

class JsonStore private(filename: String, mapper: ObjectMapper with ScalaObjectMapper) {
    def read(): Seq[MigrationStep] = {
        mapper.readValue[Seq[MigrationStep]](new File(filename))
    }

    def write(items: Seq[MigrationStep]): Unit = {
        mapper.writeValue(new File(filename), items)
    }
}

object JsonStore {

    private val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    def apply(filename: String): JsonStore = {
        new JsonStore(filename, mapper)
    }
}
