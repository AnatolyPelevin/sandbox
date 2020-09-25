package com.ringcentral.analytics.adobe.utils.reports;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.google.gson.JsonObject;
import com.ringcentral.analytics.adobe.AdobeException;
import com.ringcentral.analytics.adobe.Test;
import com.ringcentral.analytics.adobe.service.HttpService;
import com.ringcentral.analytics.adobe.utils.AdobeUtils;
import com.ringcentral.analytics.adobe.utils.reports.body.GlobalFilter;
import com.ringcentral.analytics.adobe.utils.reports.body.ReportBody;
import com.ringcentral.analytics.adobe.utils.reports.response.RootObject;
import com.ringcentral.analytics.adobe.utils.reports.response.Row;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ringcentral.analytics.adobe.utils.AdobeUtils.dateConverter;
import static com.ringcentral.analytics.adobe.utils.AdobeUtils.getDateRange;


public class ReportLoader {
    private static final Logger LOG = LoggerFactory.getLogger(ReportLoader.class);
    private final AdobeReportType adobeReportType;
    private final HttpService httpService;
    private ObjectMapper objectMapper;

    public ReportLoader(HttpService httpService, AdobeReportType adobeReportType) {
        this.adobeReportType = adobeReportType;
        this.httpService = httpService;
    }

    public AdobeReportType getAdobeReportType() {
        return adobeReportType;
    }

    public HttpService getHttpService() {
        return httpService;
    }

    private String getReportBody(String primaryReportPath, String reportPartPath) throws IOException {
        if (!Optional.ofNullable(primaryReportPath).isPresent()) {
            LOG.info("Report location in not specified!");
            return null;
        }

        if (!Optional.ofNullable(reportPartPath).isPresent()) {
            LOG.info("Report part is not specified");
            return null;
        }

        String reportBody;
        try (InputStream inputStream = getClass().getResourceAsStream("/" + primaryReportPath + "/" + reportPartPath)) {
            reportBody = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AdobeException("Cant get a body for the report: " + adobeReportType.name(), e);
        }
        return changeFiltersInReportBody(reportBody);
    }

   //TODO if all reports need the same changes, then move logic into getReportBody method
    //java 8
   private String changeFiltersInReportBody(String reportBody) throws IOException {
      switch (adobeReportType) {
           case GTM_DECK:
           case WEB_MARKETING_TRACKER:
               ReportBody reportBodyObject = getObjectMapper().readValue(reportBody, ReportBody.class);
               List<GlobalFilter> globalFilters  = reportBodyObject.getGlobalFilters();
                if (Optional.ofNullable(globalFilters).isPresent()){
                    globalFilters.forEach(globalFilter -> {
                        String dateRange = globalFilter.getDateRange();
                        if (Optional.ofNullable(dateRange).isPresent()) {
                            int index = globalFilters.indexOf(globalFilter);
                            dateRange = getDateRange(dateRange); //format should be "2019-01-06T00:00:00.000/2020-06-14T00:00:00.000"
                            globalFilter.setDateRange(dateRange);
                            globalFilters.set(index, globalFilter);
                        }
                    });
                }
                reportBody = getObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(reportBodyObject);
       }
       return reportBody;
   }
   ////java 14
//    private String changeFiltersInReportBody(String reportBody) throws IOException {
//        return switch (adobeReportType) {
//
//          case GTM_DECK, WEB_MARKETING_TRACKER -> {
//                ObjectReader reportBodyObject = getObjectMapper().readValue(reportBody, ReportBody.class);
//                var globalFilters  = reportBodyObject.getGlobalFilters();
//                if (Optional.ofNullable(globalFilters).isPresent()){
//                    globalFilters.forEach(globalFilter -> {
//                        var dateRange = globalFilter.getDateRange();
//                        if (Optional.ofNullable(dateRange).isPresent()) {
//                            var index = globalFilters.indexOf(globalFilter);
//                            dateRange = getDateRange(dateRange); //format should be "2019-01-06T00:00:00.000/2020-06-14T00:00:00.000"
//                            globalFilter.setDateRange(dateRange);
//                            globalFilters.set(index, globalFilter);
//                        }
//                    });
//                }
//                reportBody = getObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(reportBodyObject);
//                yield reportBody;
//            }
//            default -> reportBody;
//        };
//    }

    public List<String> load() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final String primaryReportPath = adobeReportType.getReportPath();
        final Class enumReport = adobeReportType.getEnumClass();

        Method methodGetReportLoadOrder = enumReport.getDeclaredMethod("getReportLoadOrder");
        Object objReportForLoading = methodGetReportLoadOrder.invoke(null);
        List<?> listReportForLoading = AdobeUtils.convertObjectToList(objReportForLoading);
        List<List<Row>> reportPartData = new ArrayList<>();

        listReportForLoading.stream()
                .map(Object::toString)
                .collect(Collectors.toList())
                .forEach(reportPath -> {
                    try {
                        String reportBody = getReportBody(primaryReportPath, reportPath);
                        reportPartData.add(processReportPart(reportBody));
                    } catch (IOException e) {
                        throw new AdobeException("Cant get all parts for the report: " + adobeReportType.name(), e);
                    }
                });
        List<String> result = convertReportToString(reportPartData);
        System.out.println(result);
        return result;
    }

    private List<Row> processReportPart(String reportBody) throws IOException {
        Boolean isLastPage;
        List<Row> reportPartData = new ArrayList<>();
        do {
            JSONObject response = httpService.getReport(reportBody);
            RootObject reportPart = getObjectMapper().readValue(response.toString(), RootObject.class);
            reportPartData.addAll(reportPart.getRows());
            isLastPage = reportPart.getLastPage();
            JSONObject reportBodyJSON = new JSONObject(reportBody);
            JSONObject pageRequested = (JSONObject) reportBodyJSON.get("settings");
            pageRequested.put("page", pageRequested.getInt("page") + 1);
            reportBody = reportBodyJSON.toString();
        } while (!isLastPage);

        return reportPartData;
    }

    private List<String> convertReportToString(List<List<Row>> reportPartData) {
        List<Row> list = AdobeUtils.concatenate(reportPartData);

//        String result = list.stream()
//                .collect(Collectors.groupingBy(it -> it.getValue()))
//                .entrySet()
//                .stream()
//                .map(it -> "\"" + it.getKey() + "\"" + ", " + it.getValue()
//                        .stream()
//                        .flatMap(i -> i.getData()
//                                .stream())
//                        .map(Object::toString)
//                        .collect(Collectors.joining(", ")))
//                .collect(Collectors.joining("\n"));
        List<String> result = list.stream()
                .collect(Collectors.groupingBy(it -> it.getValue()))
                .entrySet()
                .stream()
                .map(it -> dateConverter(it.getKey()) + ", " + it.getValue()
                        .stream()
                        .flatMap(i -> i.getData()
                                .stream())
                        .map(Object::toString)
                        .collect(Collectors.joining(", ")))
                .collect(Collectors.toList());
        return result;
    }


    private ObjectMapper getObjectMapper() {
        if (!Optional.ofNullable(objectMapper).isPresent()) {
            objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
        return objectMapper;
    }
}
