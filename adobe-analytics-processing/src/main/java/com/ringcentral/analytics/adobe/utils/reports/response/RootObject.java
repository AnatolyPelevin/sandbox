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
        "totalPages",
        "firstPage",
        "lastPage",
        "numberOfElements",
        "number",
        "totalElements",
        "columns",
        "rows",
        "summaryData"
})

public class RootObject {

    @JsonProperty("totalPages")
    private Integer totalPages;
    @JsonProperty("firstPage")
    private Boolean firstPage;
    @JsonProperty("lastPage")
    private Boolean lastPage;
    @JsonProperty("numberOfElements")
    private Integer numberOfElements;
    @JsonProperty("number")
    private Integer number;
    @JsonProperty("totalElements")
    private Integer totalElements;
    @JsonProperty("columns")
    private Columns columns;
    @JsonProperty("rows")
    private List<Row> rows = null;
    @JsonProperty("summaryData")
    private SummaryData summaryData;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("totalPages")
    public Integer getTotalPages() {
        return totalPages;
    }

    @JsonProperty("totalPages")
    public void setTotalPages(Integer totalPages) {
        this.totalPages = totalPages;
    }

    @JsonProperty("firstPage")
    public Boolean getFirstPage() {
        return firstPage;
    }

    @JsonProperty("firstPage")
    public void setFirstPage(Boolean firstPage) {
        this.firstPage = firstPage;
    }

    @JsonProperty("lastPage")
    public Boolean getLastPage() {
        return lastPage;
    }

    @JsonProperty("lastPage")
    public void setLastPage(Boolean lastPage) {
        this.lastPage = lastPage;
    }

    @JsonProperty("numberOfElements")
    public Integer getNumberOfElements() {
        return numberOfElements;
    }

    @JsonProperty("numberOfElements")
    public void setNumberOfElements(Integer numberOfElements) {
        this.numberOfElements = numberOfElements;
    }

    @JsonProperty("number")
    public Integer getNumber() {
        return number;
    }

    @JsonProperty("number")
    public void setNumber(Integer number) {
        this.number = number;
    }

    @JsonProperty("totalElements")
    public Integer getTotalElements() {
        return totalElements;
    }

    @JsonProperty("totalElements")
    public void setTotalElements(Integer totalElements) {
        this.totalElements = totalElements;
    }

    @JsonProperty("columns")
    public Columns getColumns() {
        return columns;
    }

    @JsonProperty("columns")
    public void setColumns(Columns columns) {
        this.columns = columns;
    }

    @JsonProperty("rows")
    public List<Row> getRows() {
        return rows;
    }

    @JsonProperty("rows")
    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    @JsonProperty("summaryData")
    public SummaryData getSummaryData() {
        return summaryData;
    }

    @JsonProperty("summaryData")
    public void setSummaryData(SummaryData summaryData) {
        this.summaryData = summaryData;
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