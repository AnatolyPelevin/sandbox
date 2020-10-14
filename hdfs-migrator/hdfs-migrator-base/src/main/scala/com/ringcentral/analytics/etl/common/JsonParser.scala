package com.ringcentral.analytics.etl.common

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonParser {

    def toJson[T](obj: T): String = {
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        mapper.writeValueAsString(obj)
    }

    def jsonToAnyMap(json: String): Map[String, Any] = {
        if (json == null || json.isEmpty) {
            return Map.empty[String, Any]
        }

        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        val optType = new TypeReference[Map[String, Any]]() {}
        mapper.readValue(json, optType)
    }

    def jsonToStringMap(json: String): Map[String, String] = {
        if (json == null || json.isEmpty) {
            return Map.empty[String, String]
        }

        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        val optType = new TypeReference[Map[String, String]]() {}
        mapper.readValue(json, optType)
    }

}
