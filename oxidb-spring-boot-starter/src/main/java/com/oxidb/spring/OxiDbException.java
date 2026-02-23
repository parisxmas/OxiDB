package com.oxidb.spring;

/**
 * Raised when the OxiDB server returns an error response.
 */
public class OxiDbException extends RuntimeException {
    public OxiDbException(String message) {
        super(message);
    }

    public OxiDbException(String message, Throwable cause) {
        super(message, cause);
    }
}
