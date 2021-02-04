package com.ringcentral.analytics.cloudera.service;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.ringcentral.analytics.cloudera.ClouderaException;
import com.ringcentral.analytics.cloudera.utils.response.TableResponse;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;


public class HttpService {
    private static final Logger LOG = LoggerFactory.getLogger(HttpService.class);

    private final String baseUrl;
    private final HttpClient httpClient;
    private final String clouderaUser;
    private final String clouderaUserPassword;
    private final String databaseName;
    private final String startTime;
    private final String endTime;
    private final String defaultQuery;

    private  String baseEncode;
    private URIBuilder builder;



    public HttpService(HttpClient httpClient, String baseUrl,  String clouderaUser, String clouderaUserPassword, String databaseName, String startTime, String endTime) throws UnsupportedEncodingException, URISyntaxException {
        this.httpClient = httpClient;
        this.baseUrl = baseUrl;
        this.clouderaUser = clouderaUser;
        this.clouderaUserPassword = clouderaUserPassword;
        this.databaseName = databaseName;
        this.startTime = startTime;
        this.endTime = endTime;


        defaultQuery = prepareQueryParam(databaseName,null);

        this.builder = new URIBuilder(baseUrl);
        builder.setPath("/api/v13/audits")
                .setParameter("query", defaultQuery)
                .setParameter("startTime", startTime)
                .setParameter("endTime", endTime)
                .setParameter("offset", "0");

    }

    public List<TableResponse> getTableUsage(int offset, String startTime, String endTime, String tableName) throws URISyntaxException {
        builder.setParameter("offset", Integer.toString(offset));
        if (Optional.ofNullable(startTime).isPresent()){
            builder.setParameter("startTime", startTime);
        } else {
            builder.setParameter("startTime", this.startTime);
        }
        if (Optional.ofNullable(endTime).isPresent()){
            builder.setParameter("tableName", endTime);
        } else {
            builder.setParameter("tableName", this.endTime);
        }
        if (Optional.ofNullable(tableName).isPresent()){
            builder.setParameter("query", prepareQueryParam(databaseName, tableName));
        } else {
            builder.setParameter("query", defaultQuery);
        }

        HttpGet getRequestUsage = new HttpGet(builder.build());

        getRequestUsage.addHeader("Authorization", "Basic " + baseEncode());
        getRequestUsage.addHeader("Content-Type", "application/json");


        try {
            HttpResponse response = httpClient.execute(getRequestUsage);
            String responseString = EntityUtils.toString(response.getEntity());
            //List<String> list = new ArrayList<>();
            List<TableResponse> tableResponses = new ArrayList<>();
            if (Optional.ofNullable(responseString).isPresent()){
                Gson gson = new Gson();
                TypeToken<List<TableResponse>> token = new TypeToken<List<TableResponse>>() {};
                tableResponses = gson.fromJson(responseString, token.getType());
               // list = tableResponses.stream().map(TableResponse::toString).collect(Collectors.toList());
            }
            return tableResponses;
        } catch (IOException e) {
            throw new ClouderaException("Get table info error", e);
        }
    }

    private String prepareQueryParam (String databaseName, String tableName) {
       StringBuilder builder =  new StringBuilder("username!=*bigdata.qa*;username!=*bigdata.qa@RINGCENTRAL.COM*;username!=*ERD_dev*;")
                .append("database_name==")
                .append(databaseName);

        if (Optional.ofNullable(tableName).isPresent()){
            builder.append(";")
                    .append("table_name==")
                    .append(tableName);
        }
        return builder.toString();
    }

       private String baseEncode() {
        if (!Optional.ofNullable(baseEncode).isPresent()) {
            String encode = clouderaUser + ":" + clouderaUserPassword;
            Base64.Encoder encoder = Base64.getEncoder();
            baseEncode = encoder.encodeToString(encode.getBytes());
        }
        return baseEncode;
    }
}
