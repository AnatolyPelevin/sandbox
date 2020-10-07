package generator.utils

import generator.model.{Message, RequestType}
import generator.GenOptions

class MessageGenerator(options: GenOptions, sparkUtils: SparkUtils) {

    def compose(tableName: String): List[Message] = {

        if ("".equals(options.partitionSpec)) {
            return getNonPartitionedList(tableName)
        }

        val tablePartitions = sparkUtils.getAllTablePartitions(tableName)
        if (tablePartitions.isEmpty) {
            return getNonPartitionedList(tableName)
        }

        val matchingPartitions = tablePartitions
            .filter(part => part.matches(options.partitionSpec))

        if (matchingPartitions.isEmpty) {
            return List.empty
        }

        val firstPartition = matchingPartitions.head
        val firstPartitionPath = sparkUtils.getSamplePartitionUrl(tableName, partitionCorrected(firstPartition), true)

        matchingPartitions.map(
            partitionSpec => new Message(
                RequestType.valueOf(options.requestType),
                firstPartitionPath.replace(firstPartition, partitionSpec),
                options.schemaName,
                tableName,
                partitionSpec
            )
        )
    }

    def getNonPartitionedList(tableName: String): List[Message] = {
        List(new Message(
            RequestType.valueOf(options.requestType),
            sparkUtils.getSamplePartitionUrl(tableName, "", false),
            options.schemaName,
            tableName,
            ""
        ))
    }

    def partitionCorrected(partition: String): String = {
        partition.replace("/", "',").replace("=", "='").concat("'")
    }
}
