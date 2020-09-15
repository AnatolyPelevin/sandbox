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
        "filteredTotals",
        "totals",
        "col-max",
        "col-min"
})
public class SummaryData {

    @JsonProperty("filteredTotals")
    private List<Double> filteredTotals = null;
    @JsonProperty("totals")
    private List<Double> totals = null;
    @JsonProperty("col-max")
    private List<Double> colMax = null;
    @JsonProperty("col-min")
    private List<Double> colMin = null;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("filteredTotals")
    public List<Double> getFilteredTotals() {
        return filteredTotals;
    }

    @JsonProperty("filteredTotals")
    public void setFilteredTotals(List<Double> filteredTotals) {
        this.filteredTotals = filteredTotals;
    }

    @JsonProperty("totals")
    public List<Double> getTotals() {
        return totals;
    }

    @JsonProperty("totals")
    public void setTotals(List<Double> totals) {
        this.totals = totals;
    }

    @JsonProperty("col-max")
    public List<Double> getColMax() {
        return colMax;
    }

    @JsonProperty("col-max")
    public void setColMax(List<Double> colMax) {
        this.colMax = colMax;
    }

    @JsonProperty("col-min")
    public List<Double> getColMin() {
        return colMin;
    }

    @JsonProperty("col-min")
    public void setColMin(List<Double> colMin) {
        this.colMin = colMin;
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