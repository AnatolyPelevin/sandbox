package com.ringcentral.analytics.adobe;

public class AdobeException extends RuntimeException {
    private final int status;

    public AdobeException(String message) {
        super(message);
        this.status = 0;
    }

    public AdobeException(String message, int status) {
        super(message);
        this.status = status;
    }

    public AdobeException(String message, Throwable cause) {
        super(message, cause);
        this.status = 0;
    }

    public int getStatus() {
        return status;
    }
}