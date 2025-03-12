package com.dststore.replica;

/**
 * Represents the types of operations that can be performed on the distributed store.
 * Used to distinguish between read (GET) and write (PUT) operations in request handling.
 */
public enum OperationType {
    GET, 
    PUT;
    
    @Override
    public String toString() {
        return name();
    }
} 