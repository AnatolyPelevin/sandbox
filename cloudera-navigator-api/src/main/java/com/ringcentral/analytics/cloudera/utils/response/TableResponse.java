package com.ringcentral.analytics.cloudera.utils.response;

import com.fasterxml.jackson.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "timestamp",
        "service",
        "username",
        "ipAddress",
        "command",
        "resource",
        "operationText",
        "allowed",
        "serviceValues"
})
public class TableResponse {
    @JsonProperty("timestamp")
    private String timestamp;
    @JsonProperty("service")
    private String service;
    @JsonProperty("username")
    private String username;
    @JsonProperty("ipAddress")
    private String ipAddress;
    @JsonProperty("command")
    private String command;
    @JsonProperty("resource")
    private String resource;
    @JsonProperty("operationText")
    private String operationText;
    @JsonProperty("allowed")
    private Boolean allowed;
    @JsonProperty("serviceValues")
    private ServiceValues serviceValues;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("timestamp")
    public String getTimestamp() {
        return timestamp;
    }

    @JsonProperty("timestamp")
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @JsonProperty("service")
    public String getService() {
        return service;
    }

    @JsonProperty("service")
    public void setService(String service) {
        this.service = service;
    }

    @JsonProperty("username")
    public String getUsername() {
        return username;
    }

    @JsonProperty("username")
    public void setUsername(String username) {
        this.username = username;
    }

    @JsonProperty("ipAddress")
    public String getIpAddress() {
        return ipAddress;
    }

    @JsonProperty("ipAddress")
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    @JsonProperty("command")
    public String getCommand() {
        return command;
    }

    @JsonProperty("command")
    public void setCommand(String command) {
        this.command = command;
    }

    @JsonProperty("resource")
    public String getResource() {
        return resource;
    }

    @JsonProperty("resource")
    public void setResource(String resource) {
        this.resource = resource;
    }

    @JsonProperty("operationText")
    public String getOperationText() {
        return operationText;
    }

    @JsonProperty("operationText")
    public void setOperationText(String operationText) {
        this.operationText = operationText;
    }

    @JsonProperty("allowed")
    public Boolean getAllowed() {
        return allowed;
    }

    @JsonProperty("allowed")
    public void setAllowed(Boolean allowed) {
        this.allowed = allowed;
    }

    @JsonProperty("serviceValues")
    public ServiceValues getServiceValues() {
        return serviceValues;
    }

    @JsonProperty("serviceValues")
    public void setServiceValues(ServiceValues serviceValues) {
        this.serviceValues = serviceValues;
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
        return new StringBuilder(Optional.ofNullable(timestamp).orElse(""))
                .append(",")
                .append(Optional.ofNullable(service).orElse(""))
                .append(",")
                .append(Optional.ofNullable(username).orElse(""))
                .append(",")
                .append(Optional.ofNullable(ipAddress).orElse(""))
                .append(",")
                .append(Optional.ofNullable(command).orElse(""))
                .append(",")
                .append(Optional.ofNullable(resource).orElse(""))
                .append(",")
//                .append(operationText)
//                .append(",")
                .append(Optional.ofNullable(allowed).orElse(Boolean.valueOf("FALSE")))
                .append(",")
                .append(serviceValues)
                .toString();
    }
}
