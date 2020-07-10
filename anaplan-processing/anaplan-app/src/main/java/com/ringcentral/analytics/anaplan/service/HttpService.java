package com.ringcentral.analytics.anaplan.service;

import com.ringcentral.analytics.anaplan.AnaplanException;
import com.ringcentral.analytics.anaplan.utils.AnaplanExportFile;
import com.ringcentral.analytics.anaplan.utils.AnaplanExportStatus;
import com.ringcentral.analytics.anaplan.utils.AnaplanOperation;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.apache.http.message.BasicHeader;

import static com.ringcentral.analytics.anaplan.utils.AnaplanUtils.getEnumFromString;


public class HttpService {
    private static final Logger LOG = LoggerFactory.getLogger(HttpService.class);
    private static final int REFRESH_TOKEN_PERIOD = 1800000; //should refresh token in 30 min
    private static final int AUTHENTICATION_THRESHOLD_MS = 5000;
    private static final int TOKEN_CAN_BE_REFRESHED = 8; //can refresh 8 times, then should create a new one
    private final String clientName;
    private final String password;
    private final String baseAuthUrl;
    private final String baseUrl;
    private final HttpClient httpClient;
    private List<Header> headers;
    private String token;
    private long authenticationExpirationTime;
    private long tokenRefreshed = 0;

    public HttpService(HttpClient httpClient, String baseAuthUrl, String baseUrl, String clientName, String password, List<AnaplanExportFile> anaplanExportFileList) {
        this.httpClient = httpClient;
        this.baseAuthUrl = baseAuthUrl;
        this.baseUrl = baseUrl;
        this.clientName = clientName;
        this.password = password;
        if (Optional.ofNullable(anaplanExportFileList).isPresent()) {
            getToken(null);
            anaplanExportFileList.forEach(anaplanExportFile -> doExport(anaplanExportFile));
        }
    }

    private String baseEncode () {
        String encode = clientName + ":" + password;
        Base64.Encoder encoder = Base64.getEncoder();
        return encoder.encodeToString(encode.getBytes());
    }

    private void getToken(String token) {
        LOG.info("Sending authentication request");
        String authUrl = baseAuthUrl;
        HttpPost postRequestGetToken;
        if (!Optional.ofNullable(token).isPresent()){
            tokenRefreshed = 0;
            authUrl = authUrl + "authenticate";
            postRequestGetToken = new HttpPost(authUrl);
            postRequestGetToken.addHeader("Authorization", "Basic " + baseEncode());
        } else {
            authUrl = authUrl + "refresh";
            postRequestGetToken = new HttpPost(authUrl);
            postRequestGetToken.addHeader("Authorization", "AnaplanAuthToken " + token);
        }

        postRequestGetToken.addHeader("Content-Type", "application/json");

        try {
            long requestTime = System.currentTimeMillis();
            HttpResponse response = httpClient.execute(postRequestGetToken);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
                throw new AnaplanException("Authentication failed, HTTP error code : " + response.getStatusLine().getStatusCode());
            }

            String responseString = EntityUtils.toString(response.getEntity());
            JSONObject responseJson = new JSONObject(responseString);

            String responseStatus = responseJson.getString("status");
            if (!"SUCCESS".equals(responseStatus))  {
                throw new AnaplanException("Authentication failed, HTTP error code : " + responseStatus);
            }

            setToken(responseJson.getJSONObject("tokenInfo").getString("tokenValue"));
            LOG.info("New Token has been recieved.");
            authenticationExpirationTime = requestTime + REFRESH_TOKEN_PERIOD - AUTHENTICATION_THRESHOLD_MS;
            tokenRefreshed++;
            headers = new ArrayList<>();
            headers.add(new BasicHeader("Authorization", "AnaplanAuthToken " + getToken()));
            headers.add(new BasicHeader("Accept-Encoding", "gzip,deflate"));
            headers.add(new BasicHeader("Content-Type", "application/json"));

        } catch (IOException e) {
            throw new AnaplanException("Authentication error", e);
        }
    }

    public void doExport(AnaplanExportFile anaplanExportFile){
        anaplanExportFile.anaplanExportFileCheck(AnaplanOperation.EXPORT);
        if (!Optional.ofNullable(token).isPresent()){
            throw new AnaplanException("Can perform export. Do not have token!");
        }

        String authUrl = baseUrl + "workspaces/" + anaplanExportFile.getWorkspaceID()
                + "/models/" + anaplanExportFile.getModelID()
                +"/exports/" + anaplanExportFile.getExportID() + "/tasks";

        LOG.info("Do export by url {}", baseUrl);
        LOG.info("Headers: {}", headers);

        checkAuthentication();

        HttpPost postRequestDoExport  = new HttpPost(authUrl);
        postRequestDoExport.setHeaders(headers.stream().toArray(Header[]::new));


        try {
            StringEntity stringEntity = new StringEntity("{\"localeName\": \"en_US\"}");
            postRequestDoExport.setEntity(stringEntity);
            HttpResponse response = httpClient.execute(postRequestDoExport);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new AnaplanException("Export failed, HTTP error code : " + response.getStatusLine().getStatusCode());
            }
             String responseString = EntityUtils.toString(response.getEntity());
            JSONObject responseJson = new JSONObject(responseString);

            String responseStatus = responseJson.getJSONObject("status").getString("message");

            if (!"SUCCESS".equals(responseStatus.toUpperCase()))  {
                throw new AnaplanException("Export failed, HTTP error code : " + responseStatus);
            }

            String taskID = responseJson.getJSONObject("task").getString("taskId");
            String status = responseJson.getJSONObject("task").getString("taskState");

            LOG.info("Export taskid = {} , status ={}", taskID, status);

            anaplanExportFile.setTaskID(taskID);

            checkResponseStatus(status, anaplanExportFile);

        } catch (IOException e) {
            throw new AnaplanException("Export error", e);
        }
    }

    public void pollingExport(AnaplanExportFile anaplanExportFile){
        anaplanExportFile.anaplanExportFileCheck(AnaplanOperation.POLLING);
        if (!Optional.ofNullable(token).isPresent()){
            throw new AnaplanException("Can perform polling. Do not have token!");
        }

        String taskID = anaplanExportFile.getTaskID();

        String authUrl = baseUrl + "workspaces/" + anaplanExportFile.getWorkspaceID()
                + "/models/" + anaplanExportFile.getModelID()
                + "/exports/" + anaplanExportFile.getExportID()
                + "/tasks/" + taskID;

        LOG.info("Do polling by url {}", baseUrl);
        LOG.info("Headers: {}", headers);

        checkAuthentication();

        HttpGet pollingRequest  = new HttpGet(authUrl);
        pollingRequest.setHeaders(headers.stream().toArray(Header[]::new));


        try {
            HttpResponse response = httpClient.execute(pollingRequest);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new AnaplanException("Polling failed, HTTP error code : " + response.getStatusLine().getStatusCode());
            }

            String responseString = EntityUtils.toString(response.getEntity());
            JSONObject responseJson = new JSONObject(responseString);

            String responseStatus = responseJson.getJSONObject("status").getString("message");
            if (!"SUCCESS".equals(responseStatus.toUpperCase()))  {
                throw new AnaplanException("Polling failed, HTTP error code : " + responseStatus);
            }

            String taskState = responseJson.getJSONObject("task").getString("taskState");
            LOG.info("Polling taskid = {} state  = {} ", taskID, taskState);

            checkResponseStatus(taskState, anaplanExportFile);

        } catch (IOException e) {
            throw new AnaplanException("Polling error", e);
        }
    }

    private void checkResponseStatus(String taskState, AnaplanExportFile anaplanExportFile){
        AnaplanExportStatus status = getEnumFromString(AnaplanExportStatus.class, taskState);

        if (!Optional.ofNullable(status).isPresent()){
            throw new AnaplanException("Can perform export operation. Status is unvalid!");
        }

        switch (status) {
            case NOT_STARTED:
            case IN_PROGRESS:
                pollingExport(anaplanExportFile);
                break;
            case COMPLETE:
                int chunks = getFileChunks(anaplanExportFile);
                if (chunks >= 0) for (int i = 0; i <= (chunks - 1); i++)
                    doDownload(anaplanExportFile, i);
                return;
            default:
                throw new AnaplanException("Export was cancelled!");
        }

    }

    private int getFileChunks(AnaplanExportFile anaplanExportFile) {
        anaplanExportFile.anaplanExportFileCheck(AnaplanOperation.GET_CHUNKS);
        if (!Optional.ofNullable(token).isPresent()){
            throw new AnaplanException("Can perform get chunks. Do not have token!");
        }

        String authUrl = baseUrl
                + "workspaces/" + anaplanExportFile.getWorkspaceID()
                + "/models/" + anaplanExportFile.getModelID()
                + "/files/" + anaplanExportFile.getFileID() + "/chunks";

        LOG.info("Do get chunks by url {}", baseUrl);
        LOG.info("Headers: {}", headers);

        checkAuthentication();

        HttpGet getChunks  = new HttpGet(authUrl);
        getChunks.setHeaders(headers.stream().toArray(Header[]::new));


        try {
            HttpResponse response = httpClient.execute(getChunks);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new AnaplanException("Get chunks failed, HTTP error code : " + response.getStatusLine().getStatusCode());
            }
            String responseString = EntityUtils.toString(response.getEntity());
            JSONObject responseJson = new JSONObject(responseString);

            String responseStatus = responseJson.getJSONObject("status").getString("message");
            if (!"SUCCESS".equals(responseStatus.toUpperCase()))  {
                throw new AnaplanException("Get chunks failed, HTTP error code : " + responseStatus);
            }

            JSONArray chunksArray = responseJson.getJSONArray("chunks");
            return  Optional.ofNullable(chunksArray).isPresent() ? chunksArray.length() : -1;

        } catch (IOException e) {
            throw new AnaplanException("Export error", e);
        }
    }

    private void doDownload(AnaplanExportFile anaplanExportFile, int chunkNumber) {
        anaplanExportFile.anaplanExportFileCheck(AnaplanOperation.DOWNLOAD);
        if (!Optional.ofNullable(token).isPresent()){
            throw new AnaplanException("Can perform download chunks. Do not have token!");
        }

        String authUrl = baseUrl
                + "workspaces/" + anaplanExportFile.getWorkspaceID()
                + "/models/" + anaplanExportFile.getModelID()
                + "/files/" + anaplanExportFile.getFileID()
                + "/chunks/" + chunkNumber;

        LOG.info("Do download by url {}", baseUrl);
        LOG.info("Headers: {}", headers);

        checkAuthentication();

        HttpGet getChunks  = new HttpGet(authUrl);
        getChunks.setHeaders(headers.stream().toArray(Header[]::new));


        try {
            HttpResponse response = httpClient.execute(getChunks);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new AnaplanException("Download failed, HTTP error code : " + response.getStatusLine().getStatusCode());
            }
            String responseString = EntityUtils.toString(response.getEntity());
            System.out.println(responseString);
            //JSONObject responseJson = new JSONObject(responseString);

        } catch (IOException e) {
            throw new AnaplanException("Export error", e);
        }

    }


    private void checkAuthentication() {
        if (System.currentTimeMillis() >= authenticationExpirationTime) {
            try {
                Thread.sleep(AUTHENTICATION_THRESHOLD_MS * 2);
            } catch (InterruptedException e) {
                LOG.error("Error during execution occurred: {}", e.getMessage());
            }
            if (tokenRefreshed >= TOKEN_CAN_BE_REFRESHED) {
                getToken(null);
            }
            getToken(token);
        }
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }


}
