package generator.model;

import java.util.Objects;

public class Message {
    private final RequestType requestType;
    private final String sourcePath;
    private final String schemaName;
    private final String tableName;
    private final String partitionSpec;

    public Message(RequestType requestType,
                   String sourcePath,
                   String schemaName,
                   String tableName,
                   String partitionSpec) {
        this.requestType = requestType;
        this.sourcePath = sourcePath;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.partitionSpec = partitionSpec;
    }

    @Override
    public String toString() {
        String result = "";
        String basicMessage = "{\"requestType\":\""
            .concat(requestType.name())
            .concat("\",\n\"sourcePath\":\"")
            .concat(sourcePath)
            .concat("\",\n\"schemaName\":\"")
            .concat(schemaName)
            .concat("\",\n\"tableName\":\"")
            .concat(tableName);
        if ("".equals(partitionSpec)) {
            result = basicMessage.concat("\"}");
        } else {
            result = basicMessage.concat("\",\n\"partitionSpec\":\"")
                .concat(partitionSpec)
                .concat("\"}");
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message)) return false;
        Message message = (Message) o;
        return requestType == message.requestType &&
            sourcePath.equals(message.sourcePath) &&
            schemaName.equals(message.schemaName) &&
            tableName.equals(message.tableName) &&
            partitionSpec.equals(message.partitionSpec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestType, sourcePath, schemaName, tableName, partitionSpec);
    }
}