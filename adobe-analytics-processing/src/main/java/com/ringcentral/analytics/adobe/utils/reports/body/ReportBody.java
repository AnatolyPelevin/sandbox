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
        "rsid",
        "globalFilters",
        "metricContainer",
        "dimension",
        "settings",
        "statistics"
})
public class ReportBody {
    @JsonProperty("rsid")
    private String rsid;
    @JsonProperty("globalFilters")
    private List<GlobalFilter> globalFilters = null;
    @JsonProperty("metricContainer")
    private MetricContainer metricContainer;
    @JsonProperty("dimension")
    private String dimension;
    @JsonProperty("settings")
    private Settings settings;
    @JsonProperty("statistics")
    private Statistics statistics;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<>();

    @JsonProperty("rsid")
    public String getRsid() {
        return rsid;
    }

    @JsonProperty("rsid")
    public void setRsid(String rsid) {
        this.rsid = rsid;
    }

    @JsonProperty("globalFilters")
    public List<GlobalFilter> getGlobalFilters() {
        return globalFilters;
    }

    @JsonProperty("globalFilters")
    public void setGlobalFilters(List<GlobalFilter> globalFilters) {
        this.globalFilters = globalFilters;
    }

    @JsonProperty("metricContainer")
    public MetricContainer getMetricContainer() {
        return metricContainer;
    }

    @JsonProperty("metricContainer")
    public void setMetricContainer(MetricContainer metricContainer) {
        this.metricContainer = metricContainer;
    }

    @JsonProperty("dimension")
    public String getDimension() {
        return dimension;
    }

    @JsonProperty("dimension")
    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    @JsonProperty("settings")
    public Settings getSettings() {
        return settings;
    }

    @JsonProperty("settings")
    public void setSettings(Settings settings) {
        this.settings = settings;
    }

    @JsonProperty("statistics")
    public Statistics getStatistics() {
        return statistics;
    }

    @JsonProperty("statistics")
    public void setStatistics(Statistics statistics) {
        this.statistics = statistics;
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
