package com.ringcentral.analytics.adobe.utils.reports.body;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "columnId",
        "id",
        "filters"
})
public class Metric {

    @JsonProperty("columnId")
    private String columnId;
    @JsonProperty("id")
    private String id;
    @JsonProperty("filters")
    private List<String> filters = null;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("columnId")
    public String getColumnId() {
        return columnId;
    }

    @JsonProperty("columnId")
    public void setColumnId(String columnId) {
        this.columnId = columnId;
    }

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty("filters")
    public List<String> getFilters() {
        return filters;
    }

    @JsonProperty("filters")
    public void setFilters(List<String> filters) {
        this.filters = filters;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}
