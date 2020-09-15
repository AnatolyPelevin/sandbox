package com.ringcentral.analytics.adobe.utils.reports.body;

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "countRepeatInstances",
        "limit",
        "page",
        "dimensionSort",
        "nonesBehavior"
})
public class Settings {

    @JsonProperty("countRepeatInstances")
    private Boolean countRepeatInstances;
    @JsonProperty("limit")
    private Integer limit;
    @JsonProperty("page")
    private Integer page;
    @JsonProperty("dimensionSort")
    private String dimensionSort;
    @JsonProperty("nonesBehavior")
    private String nonesBehavior;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("countRepeatInstances")
    public Boolean getCountRepeatInstances() {
        return countRepeatInstances;
    }

    @JsonProperty("countRepeatInstances")
    public void setCountRepeatInstances(Boolean countRepeatInstances) {
        this.countRepeatInstances = countRepeatInstances;
    }

    @JsonProperty("limit")
    public Integer getLimit() {
        return limit;
    }

    @JsonProperty("limit")
    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    @JsonProperty("page")
    public Integer getPage() {
        return page;
    }

    @JsonProperty("page")
    public void setPage(Integer page) {
        this.page = page;
    }

    @JsonProperty("dimensionSort")
    public String getDimensionSort() {
        return dimensionSort;
    }

    @JsonProperty("dimensionSort")
    public void setDimensionSort(String dimensionSort) {
        this.dimensionSort = dimensionSort;
    }

    @JsonProperty("nonesBehavior")
    public String getNonesBehavior() {
        return nonesBehavior;
    }

    @JsonProperty("nonesBehavior")
    public void setNonesBehavior(String nonesBehavior) {
        this.nonesBehavior = nonesBehavior;
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
