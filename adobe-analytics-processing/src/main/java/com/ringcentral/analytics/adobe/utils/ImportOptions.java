package com.ringcentral.analytics.adobe.utils;


import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class ImportOptions {
    private static final OptionParser PARSER = createOptionParser();

    private static final String TMP_LOCATION = "tmp-location";

    private static final String TLS_VERSION = "tls-version";

    /**
     Go to https://console.adobe.io -> Select your integration and copy the client credential from "Overview"
     */
    /** API Key (Client ID)*/
    private static final String ADOBE_JWT_CLIENT_ID = "adobe-jwt-client-id";
    /** Client secret*/
    private static final String ADOBE_JWT_CLIENT_SECRET = "adobe-jwt-client-secret";
    /** Technical account ID */
    private static final String ADOBE_TECHNICAL_ACCOUNT_ID = "adobe-tech-acc-id";
    /**Organization ID*/
    private static final String ADOBE_ORG_ID = "adobe-org-id";
    /**From "JWT" section of the integration scopes e.g.
     * ent_analytics_bulk_ingest_sdk from "https://ims-na1.adobelogin.com/s/ent_analytics_bulk_ingest_sdk"*/
    private static final String ADOBE_METASCOPES = "adobe-metascopes";
    /**Path to secret.key file for the certificate uploaded in console.adobe.io integration*/
    private static final String ADOBE_KEY_PATH = "adobe-key-path";

    /** URL Endpoints*/
    private static final String ADOBE_IMS_HOST = "adobe-ims-host";
    private static final String ADOBE_IMS_EXCHANGE = "adobe-ims-exchange";
    private static final String ADOBE_DISCOVERY_URL = "adobe-discovery-url";
    private static final String ADOBE_ANALYTICS_URL = "adobe-analytics-url";
    private static final String ADOBE_REPORT_URL = "adobe-report-url";

    private final String tmpLocation;

    private final String tlsVersion;

    private final String adobeJwtClientId;
    private final String adobeJwtClientSecret;
    private final String adobeTechnicalAccountId;
    private final String adobeOrgId;
    private final String adobeMetascopes;
    private final String adobeKeyPath;

    private final String adobeImsHost;
    private final String adobeImsExchange;
    private final String adobeDiscoveryUrl;
    private final String adobeAnalyticsUrl;

    public ImportOptions(final String[] args) {
        final OptionSet options = PARSER.parse(args);

        tmpLocation = options.valueOf(TMP_LOCATION).toString();

        tlsVersion = options.valueOf(TLS_VERSION).toString();

        adobeJwtClientId = options.valueOf(ADOBE_JWT_CLIENT_ID).toString();;
        adobeJwtClientSecret = options.valueOf(ADOBE_JWT_CLIENT_SECRET).toString();
        adobeOrgId = options.valueOf(ADOBE_ORG_ID).toString();
        adobeTechnicalAccountId = options.valueOf(ADOBE_TECHNICAL_ACCOUNT_ID).toString();
        adobeMetascopes = options.valueOf(ADOBE_METASCOPES).toString();
        adobeKeyPath = options.valueOf(ADOBE_KEY_PATH).toString();

        adobeImsHost = options.valueOf(ADOBE_IMS_HOST).toString();
        adobeImsExchange = options.valueOf(ADOBE_IMS_EXCHANGE).toString();
        adobeDiscoveryUrl = options.valueOf(ADOBE_DISCOVERY_URL).toString();
        adobeAnalyticsUrl = options.valueOf(ADOBE_ANALYTICS_URL).toString();
    }

    private static OptionParser createOptionParser() {
        OptionParser optionParser = new OptionParser();

     //   optionParser.accepts(TMP_LOCATION).withRequiredArg().ofType(String.class).defaultsTo("D:\\WORK\\ADOBE_API\\Adobe_auth\\tmp");
        optionParser.accepts(TMP_LOCATION).withRequiredArg().ofType(String.class).defaultsTo("C:\\Users\\anatoly.pelevin\\Documents\\HELP\\Adobe_auth\\tmp");
        optionParser.accepts(TLS_VERSION).withOptionalArg().defaultsTo("TLSv1.2");

        //TODO add cred
        optionParser.accepts(ADOBE_JWT_CLIENT_ID).withRequiredArg().ofType(String.class).defaultsTo("ebced9a044fe48178d2a86930ce1f23a");
        optionParser.accepts(ADOBE_JWT_CLIENT_SECRET).withRequiredArg().ofType(String.class).defaultsTo("757cf369-37cc-483c-a978-035dde06a3b2");
        optionParser.accepts(ADOBE_ORG_ID).withRequiredArg().ofType(String.class).defaultsTo("101A678254E6D3620A4C98A5@AdobeOrg");
        optionParser.accepts(ADOBE_TECHNICAL_ACCOUNT_ID).withRequiredArg().ofType(String.class).defaultsTo("DD2B0C7D5F3D233A0A495FC6@techacct.adobe.com");

        optionParser.accepts(ADOBE_METASCOPES).withRequiredArg().ofType(String.class).defaultsTo("ent_analytics_bulk_ingest_sdk");
        //optionParser.accepts(ADOBE_KEY_PATH).withRequiredArg().ofType(String.class).defaultsTo("D:\\WORK\\ADOBE_API\\Adobe_auth\\auth\\config\\private.key");
        optionParser.accepts(ADOBE_KEY_PATH).withRequiredArg().ofType(String.class).defaultsTo("C:\\Users\\anatoly.pelevin\\Documents\\HELP\\Adobe_auth\\auth\\config\\private.key");

        optionParser.accepts(ADOBE_IMS_HOST).withRequiredArg().ofType(String.class).defaultsTo("ims-na1.adobelogin.com");
        optionParser.accepts(ADOBE_IMS_EXCHANGE).withRequiredArg().ofType(String.class).defaultsTo("https://ims-na1.adobelogin.com/ims/exchange/jwt");
        optionParser.accepts(ADOBE_DISCOVERY_URL).withRequiredArg().ofType(String.class).defaultsTo("https://analytics.adobe.io/discovery/me");
        optionParser.accepts(ADOBE_ANALYTICS_URL).withRequiredArg().ofType(String.class).defaultsTo("https://analytics.adobe.io/api");

        return optionParser;
    }

    public String getTlsVersion() {
        return tlsVersion;
    }

    public String getAdobeJwtClientId() {
        return adobeJwtClientId;
    }

    public String getAdobeJwtClientSecret() {
        return adobeJwtClientSecret;
    }

    public String getAdobeOrgId() {
        return adobeOrgId;
    }

    public String getAdobeMetascopes() {
        return adobeMetascopes;
    }

    public String getAdobeKeyPath() {
        return adobeKeyPath;
    }

    public String getAdobeImsHost() {
        return adobeImsHost;
    }

    public String getAdobeImsExchange() {
        return adobeImsExchange;
    }

    public String getAdobeDiscoveryUrl() {
        return adobeDiscoveryUrl;
    }

    public String getAdobeAnalyticsUrl() {
        return adobeAnalyticsUrl;
    }

    public String getAdobeTechnicalAccountId() {
        return adobeTechnicalAccountId;
    }

    public String getTmpLocation() {
        return tmpLocation;
    }

}