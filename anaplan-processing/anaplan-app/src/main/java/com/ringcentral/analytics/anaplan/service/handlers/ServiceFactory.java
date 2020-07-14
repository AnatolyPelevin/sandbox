package com.ringcentral.analytics.anaplan.service.handlers;

import com.ringcentral.analytics.anaplan.AnaplanException;
import com.ringcentral.analytics.anaplan.service.HDFSService;
import com.ringcentral.analytics.anaplan.service.HttpService;
import com.ringcentral.analytics.anaplan.utils.AnaplanExportFile;
import com.ringcentral.analytics.anaplan.utils.ImportOptions;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.hadoop.conf.Configuration;

public class ServiceFactory {
    private static final int CONNECTION_TIMEOUT = 300000;
    private static final int SOCKET_TIMEOUT = 600000;

    public HDFSService createHDFS() {
        Configuration conf = new Configuration();
        return new HDFSService(conf);
    }
    public HttpService createHttpService(ImportOptions options) {
        HttpClient httpClient = createHttpClient(options.getTlsVersion());

        var anaplanExportFileArrayList =  List.of(
                new AnaplanExportFile("8a81b09e654f3edd01661b067aa42d83" , "92A3C5B1C286407F84143652CEE353C0", "116000000000", "116000000000"),
                new AnaplanExportFile("8a81b09e654f3edd01661b067aa42d83" , "16F02DE3EF4541FC9FD228AA0F99E6A9", "116000000005", "116000000005")
        );

        return new HttpService(
                httpClient,
                options.getAnaplanAuthServer(),
                options.getAnaplanServer(),
                options.getAnaplanClientName(),
                options.getAnaplanPassword(),
                anaplanExportFileArrayList
        );
    }

    private HttpClient createHttpClient(String tlsVersion) {
        try {
            SSLContext sslContext = new SSLContextBuilder()
                    .useProtocol(tlsVersion)
                    .loadTrustMaterial(null, (certificate, authType) -> true).build();

            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(CONNECTION_TIMEOUT)
                    .setConnectionRequestTimeout(CONNECTION_TIMEOUT)
                    .setSocketTimeout(SOCKET_TIMEOUT)
                    .build();

            return HttpClients.custom()
                    .setDefaultRequestConfig(requestConfig)
                    .setSslcontext(sslContext)
                    .setSSLHostnameVerifier(new NoopHostnameVerifier())
                    .setServiceUnavailableRetryStrategy(new HttpRetryStrategy())
                    .build();

        } catch (KeyManagementException | KeyStoreException | NoSuchAlgorithmException e) {
            throw new AnaplanException("Error in http client creation", e);
        }
    }

}
