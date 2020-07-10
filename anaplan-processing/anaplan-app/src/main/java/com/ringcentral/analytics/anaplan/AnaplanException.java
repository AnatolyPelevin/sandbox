package com.ringcentral.analytics.anaplan;

public class AnaplanException extends RuntimeException {
    private final int status;

    public AnaplanException(String message) {
        super(message);
        this.status = 0;
    }

    public AnaplanException(String message, int status) {
        super(message);
        this.status = status;
    }

    public AnaplanException(String message, Throwable cause) {
        super(message, cause);
        this.status = 0;
    }

    public int getStatus() {
        return status;
    }
}
