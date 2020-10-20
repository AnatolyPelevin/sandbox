package com.ringcentral.analytics.adobe.utils.reports.response;

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
        "dimension",
        "columnIds"
})
public class Columns {

    @JsonProperty("dimension")
    private Dimension dimension;
    @JsonProperty("columnIds")
    private List<String> columnIds = null;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("dimension")
    public Dimension getDimension() {
        return dimension;
    }

    @JsonProperty("dimension")
    public void setDimension(Dimension dimension) {
        this.dimension = dimension;
    }

    @JsonProperty("columnIds")
    public List<String> getColumnIds() {
        return columnIds;
    }

    @JsonProperty("columnIds")
    public void setColumnIds(List<String> columnIds) {
        this.columnIds = columnIds;
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