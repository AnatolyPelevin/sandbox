package com.ringcentral.analytics.cloudera;

import com.ringcentral.analytics.cloudera.service.HDFSService;
import com.ringcentral.analytics.cloudera.service.HttpService;
import com.ringcentral.analytics.cloudera.service.handlers.HttpRetryStrategy;
import com.ringcentral.analytics.cloudera.utils.ImportOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

public class ServiceFactory {
    private static final int CONNECTION_TIMEOUT = 300000;
    private static final int SOCKET_TIMEOUT = 600000;

    public HDFSService createHDFS() {
        Configuration conf = new Configuration();
        return new HDFSService(conf);
    }


    public HttpService createHttpService(ImportOptions options) throws IOException, URISyntaxException {
        HttpClient httpClient = createHttpClient(options.getTlsVersion());

        return new HttpService(
                httpClient,
                options.getBaseUrl(),
                options.getClouderaUser(),
                options.getClouderaUserPassword(),
                options.getDatabaseName(),
                options.getStartTime(),
                options.getEndTime()
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
            throw new ClouderaException("Error in http client creation", e);
        }
    }

}
