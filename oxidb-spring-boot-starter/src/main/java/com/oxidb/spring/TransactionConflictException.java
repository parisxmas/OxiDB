package com.oxidb.spring;

/**
 * Raised on OCC version conflict during transaction commit.
 */
public class TransactionConflictException extends OxiDbException {
    public TransactionConflictException(String message) {
        super(message);
    }
}
