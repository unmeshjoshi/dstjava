package com.dststore.quorum;

/**
 * Exception thrown when a read operation times out.
 */
public class ReadTimeoutException extends RuntimeException {
    public ReadTimeoutException(String message) {
        super(message);
    }
} 