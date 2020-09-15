package com.ringcentral.analytics.adobe.service.handlers;

import com.ringcentral.analytics.adobe.AdobeException;
import com.ringcentral.analytics.adobe.service.HDFSService;
import com.ringcentral.analytics.adobe.service.HttpService;
import com.ringcentral.analytics.adobe.service.JWTTokenService;
import com.ringcentral.analytics.adobe.utils.ImportOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.kerby.kerberos.provider.token.JwtAuthToken;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Optional;

public class ServiceFactory {
    private static final int CONNECTION_TIMEOUT = 300000;
    private static final int SOCKET_TIMEOUT = 600000;

    public HDFSService createHDFS() {
        Configuration conf = new Configuration();
        return new HDFSService(conf);
    }

    public JWTTokenService createJWTTokenService(ImportOptions options) {
        return new JWTTokenService(options);
    }

    public HttpService createHttpService(ImportOptions options, JWTTokenService jwt) throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
        String jwtToken = jwt.getJWTToken();
        if (!Optional.ofNullable(jwtToken).isPresent()) {
            throw new AdobeException("Failed to get JWT token!");
        }

        HttpClient httpClient = createHttpClient(options.getTlsVersion());

        return new HttpService(
                httpClient,
                options.getAdobeImsExchange(),
                options.getAdobeAnalyticsUrl(),
                options.getAdobeDiscoveryUrl(),
                options.getAdobeJwtClientId(),
                options.getAdobeJwtClientSecret(),
                options.getAdobeOrgId(),
                jwtToken
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
            throw new AdobeException("Error in http client creation", e);
        }
    }

}
