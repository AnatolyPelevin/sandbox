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
        "metrics",
        "metricFilters"
})
public class MetricContainer {

    @JsonProperty("metrics")
    private List<Metric> metrics = null;
    @JsonProperty("metricFilters")
    private List<MetricFilter> metricFilters = null;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("metrics")
    public List<Metric> getMetrics() {
        return metrics;
    }

    @JsonProperty("metrics")
    public void setMetrics(List<Metric> metrics) {
        this.metrics = metrics;
    }

    @JsonProperty("metricFilters")
    public List<MetricFilter> getMetricFilters() {
        return metricFilters;
    }

    @JsonProperty("metricFilters")
    public void setMetricFilters(List<MetricFilter> metricFilters) {
        this.metricFilters = metricFilters;
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