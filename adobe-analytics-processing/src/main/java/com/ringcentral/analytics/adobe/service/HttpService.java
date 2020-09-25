package com.ringcentral.analytics.adobe.service;

import com.ringcentral.analytics.adobe.AdobeException;
import com.ringcentral.analytics.adobe.utils.reports.AdobeReportType;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;


public class HttpService {
    private static final Logger LOG = LoggerFactory.getLogger(HttpService.class);

    private final String jwtToken;
    private final String baseAuthUrl;
    private final String baseUrl;
    private final String adobeDiscoveryUrl;
    private final HttpClient httpClient;
    private final String adobeJwtClientId;
    private final String adobeJwtClientSecret;
    private final String adobeOrgId;
    private String globalCompanyId;

    private List<Header> headers;

    public HttpService(HttpClient httpClient, String baseAuthUrl, String baseUrl, String adobeDiscoveryUrl,String adobeJwtClientId, String adobeJwtClientSecret, String adobeOrgId , String jwtToken) throws UnsupportedEncodingException {
        this.httpClient = httpClient;
        this.baseAuthUrl = baseAuthUrl;
        this.adobeJwtClientId = adobeJwtClientId;
        this.adobeJwtClientSecret = adobeJwtClientSecret;
        this.adobeOrgId = adobeOrgId;
        this.jwtToken = jwtToken;
        this.baseUrl = baseUrl;
        this.adobeDiscoveryUrl = adobeDiscoveryUrl;

        getToken();

    }

    private void getToken() throws UnsupportedEncodingException {
        LOG.info("Sending authentication request");
        HttpPost postRequestGetToken = new HttpPost(baseAuthUrl);

        List<BasicNameValuePair> postParameters =Arrays.asList(
                new BasicNameValuePair("client_id",adobeJwtClientId),
                new BasicNameValuePair("client_secret",adobeJwtClientSecret),
                new BasicNameValuePair("jwt_token",jwtToken));

        postRequestGetToken.setEntity(new UrlEncodedFormEntity(postParameters, "UTF-8"));
        try {
            //long requestTime = System.currentTimeMillis();
            HttpResponse response = httpClient.execute(postRequestGetToken);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new AdobeException("Authentication failed, HTTP error code : " + response.getStatusLine().getStatusCode());
            }

            String responseString = EntityUtils.toString(response.getEntity());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Response auth: " + responseString);
            }

            JSONObject responseJson = new JSONObject(responseString);
            if (!responseJson.has("access_token")){
                throw new AdobeException("No token in a response object");
            }
            String accessToken = responseJson.getString("access_token");
            LOG.info("New Token has been received.");
            // authenticationExpirationTime = requestTime + REFRESH_TOKEN_PERIOD - AUTHENTICATION_THRESHOLD_MS;
            headers = new ArrayList<>();
            headers.add(new BasicHeader("Authorization", "Bearer " + accessToken));
            headers.add(new BasicHeader("x-gw-ims-org-id", adobeOrgId));
            headers.add(new BasicHeader("x-api-key", adobeJwtClientId));
            headers.add(new BasicHeader("Content-Type", "application/json"));
            // get global  company id
            globalCompanyId = callUsersMeApi();
            if (Optional.ofNullable(globalCompanyId).isPresent()) {
                headers.add(new BasicHeader("x-proxy-global-company-id", globalCompanyId));
            }
        } catch (IOException e) {
            throw new AdobeException("Authentication error", e);
        }

    }

    public JSONObject getReport (String reportJsonString) throws UnsupportedEncodingException {
        String authUrl = baseUrl + "/" + globalCompanyId +"/reports";

        LOG.info("Do download by url {}", baseUrl);
        LOG.info("Headers: {}", headers);

        HttpPost getReport  = new HttpPost(authUrl);
        getReport.setHeaders(headers.stream().toArray(Header[]::new));

        StringEntity stringEntity = new StringEntity(reportJsonString);
        getReport.setEntity(stringEntity);
        JSONObject responseJson;
        try {
            HttpResponse response = httpClient.execute(getReport);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new AdobeException("Download failed, HTTP error code : " + response.getStatusLine().getStatusCode());
            }
           String responseString = EntityUtils.toString(response.getEntity());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Report object: " + responseString);

            }
            System.out.println(responseString);
            responseJson = new JSONObject(responseString);

        } catch (IOException e) {
            throw new AdobeException("Export error", e);
        }
        return responseJson;
    }
/**
 * check user me API and return globalCompanyId*/
    private String callUsersMeApi ()  {
        LOG.info("Do download by url {}", adobeDiscoveryUrl);
        LOG.info("Headers: {}", headers);

        HttpGet getCallMe  = new HttpGet(adobeDiscoveryUrl);
        getCallMe.setHeaders(headers.stream().toArray(Header[]::new));

        try {
            HttpResponse response = httpClient.execute(getCallMe);
            String responseString = EntityUtils.toString(response.getEntity());

            if (LOG.isDebugEnabled()){
                LOG.debug(responseString);
            }

            JSONObject responseJson = new JSONObject(responseString);
            return responseJson.getJSONArray("imsOrgs").getJSONObject(0).getJSONArray("companies").getJSONObject(0).getString("globalCompanyId");
        } catch (IOException e) {
            throw new AdobeException("Error with User me API: ", e);
        }
    }

}
