package com.dststore.network.tcp;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * Interface for handling I/O operations in the event loop.
 */
public interface IOHandler {
    /**
     * Handle an I/O operation for a selection key.
     *
     * @param key The selection key representing the I/O operation
     * @throws IOException If an I/O error occurs
     */
    void handle(SelectionKey key) throws IOException;
} 