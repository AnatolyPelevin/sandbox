package com.ringcentral.analytics.cloudera.utils.response;

import com.fasterxml.jackson.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "operation_text",
        "database_name",
        "query_id",
        "object_type",
        "session_id",
        "privilege",
        "table_name",
        "status"
})
public class ServiceValues {

    @JsonProperty("operation_text")
    private String operation_text;
    @JsonProperty("database_name")
    private String database_name;
    @JsonProperty("query_id")
    private String query_id;
    @JsonProperty("object_type")
    private String object_type;
    @JsonProperty("session_id")
    private String session_id;
    @JsonProperty("privilege")
    private String privilege;
    @JsonProperty("table_name")
    private String table_name;
    @JsonProperty("status")
    private String status;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("operation_text")
    public String getOperationText() {
        return operation_text;
    }

    @JsonProperty("operation_text")
    public void setOperationText(String operation_text) {
        this.operation_text = operation_text;
    }

    @JsonProperty("database_name")
    public String getDatabaseName() {
        return database_name;
    }

    @JsonProperty("database_name")
    public void setDatabaseName(String database_name) {
        this.database_name = database_name;
    }

    @JsonProperty("query_id")
    public String getQueryId() {
        return query_id;
    }

    @JsonProperty("query_id")
    public void setQueryId(String query_id) {
        this.query_id = query_id;
    }

    @JsonProperty("object_type")
    public String getObjectType() {
        return object_type;
    }

    @JsonProperty("object_type")
    public void setObjectType(String object_type) {
        this.object_type = object_type;
    }

    @JsonProperty("session_id")
    public String getSessionId() {
        return session_id;
    }

    @JsonProperty("session_id")
    public void setSessionId(String session_id) {
        this.session_id = session_id;
    }

    @JsonProperty("privilege")
    public String getPrivilege() {
        return privilege;
    }

    @JsonProperty("privilege")
    public void setPrivilege(String privilege) {
        this.privilege = privilege;
    }

    @JsonProperty("table_name")
    public String getTableName() {
        return table_name;
    }

    @JsonProperty("table_name")
    public void setTableName(String table_name) {
        this.table_name = table_name;
    }

    @JsonProperty("status")
    public String getStatus() {
        return status;
    }

    @JsonProperty("status")
    public void setStatus(String status) {
        this.status = status;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public String toString(){
        return new StringBuilder(Optional.ofNullable(database_name).orElse(""))
                .append(",")
                .append(Optional.ofNullable(query_id).orElse(""))
                .append(",")
                .append(Optional.ofNullable(object_type).orElse(""))
                .append(",")
                .append(Optional.ofNullable(session_id).orElse(""))
                .append(",")
                .append(Optional.ofNullable(privilege).orElse(""))
                .append(",")
                .append(Optional.ofNullable(table_name).orElse(""))
                .append(",")
               // .append(Optional.ofNullable(status.trim()).orElse(""))
                .toString();
    }
}