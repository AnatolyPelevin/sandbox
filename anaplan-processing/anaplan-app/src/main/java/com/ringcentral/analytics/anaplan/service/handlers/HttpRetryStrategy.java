package com.ringcentral.analytics.anaplan.service.handlers;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.protocol.HttpContext;

public class HttpRetryStrategy implements ServiceUnavailableRetryStrategy {

    private static final int MAX_RETRIES = 5;
    private static final int RETRY_INTERVAL = 5000;
    private final int maxRetries;
    private final long retryInterval;

    public HttpRetryStrategy(int maxRetries, int retryInterval) {
        super();
        if (maxRetries < 1) {
            throw new IllegalArgumentException("MaxRetries must be greater than 1");
        }
        if (retryInterval < 1) {
            throw new IllegalArgumentException("Retry interval must be greater than 1");
        }
        this.maxRetries = maxRetries;
        this.retryInterval = retryInterval;
    }

    public HttpRetryStrategy() {
        this(MAX_RETRIES, RETRY_INTERVAL);
    }

    public boolean retryRequest(final HttpResponse response, int executionCount, final HttpContext context) {
        return executionCount <= maxRetries &&
                response.getStatusLine().getStatusCode() >= HttpStatus.SC_INTERNAL_SERVER_ERROR;
    }

    public long getRetryInterval() {
        return retryInterval;
    }
}
