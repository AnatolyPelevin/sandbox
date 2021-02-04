package com.ringcentral.analytics.cloudera;

public class ClouderaException extends RuntimeException {
    private final int status;

    public ClouderaException(String message) {
        super(message);
        this.status = 0;
    }

    public ClouderaException(String message, int status) {
        super(message);
        this.status = status;
    }

    public ClouderaException(String message, Throwable cause) {
        super(message, cause);
        this.status = 0;
    }

    public int getStatus() {
        return status;
    }
}