package com.ringcentral.analytics.sns.request.generator.model

import com.ringcentral.analytics.sns.request.generator.model.RequestType.RequestType

class Message(requestType: RequestType,
              sourcePath: String,
              schemaName: String,
              tableName: String,
              partitionSpec: String) {

    def getSchemaName(): String = schemaName

    def getTableName(): String = tableName

    def getPartitionSpec(): String = partitionSpec

    override def toString: String = {
        val basicMessage = "{\"requestType\":\"".concat(requestType.toString)
            .concat("\",\n\"sourcePath\":\"")
            .concat(sourcePath)
            .concat("\",\n\"schemaName\":\"")
            .concat(schemaName)
            .concat("\",\n\"tableName\":\"")
            .concat(tableName)
        if ("" == partitionSpec) {
            basicMessage.concat("\"}")
        }
        else {
            basicMessage.concat("\",\n\"partitionSpec\":\"")
                .concat(partitionSpec)
                .concat("\"}")
        }
    }
}
