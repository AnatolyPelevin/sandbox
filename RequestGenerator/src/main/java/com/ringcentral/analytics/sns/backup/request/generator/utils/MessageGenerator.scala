package com.ringcentral.analytics.sns.backup.request.generator.utils

import com.ringcentral.analytics.sns.backup.request.generator.GeneratorOptions
import com.ringcentral.analytics.sns.backup.request.generator.model.{Message, RequestType}

class MessageGenerator(options: GeneratorOptions, sparkUtils: SparkUtils) {

    def compose(tableName: String): List[Message] = {

        val tablePartitions = sparkUtils.getAllTablePartitions(tableName)

        if (tablePartitions.isEmpty) {
            return List(new Message(
                RequestType.withName(options.requestType),
                sparkUtils.getSourcePath(tableName, "", false),
                options.schemaName,
                tableName,
                ""
            ))
        }

        val matchingPartitions = if (options.partitionSpec == "") {
            tablePartitions
        } else {
            tablePartitions.filter(part => part.matches(options.partitionSpec))
        }

        if (matchingPartitions.isEmpty) {
            return List.empty
        }

        matchingPartitions.map(
            partitionSpec => new Message(
                RequestType.withName(options.requestType),
                sparkUtils.getSourcePath(tableName, partitionSpec, true),
                options.schemaName,
                tableName,
                partitionSpec
            )
        )
    }
}
