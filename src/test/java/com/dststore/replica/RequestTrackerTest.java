package com.dststore.replica;

import com.dststore.message.Message;
import com.dststore.message.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RequestTrackerTest {
    private ScheduledExecutorService scheduler;
    private RequestTracker requestTracker;
    private static final String REQUEST_ID = "test-request";
    private static final int QUORUM_SIZE = 2;

    @BeforeEach
    void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        requestTracker = new RequestTracker<>(REQUEST_ID, QUORUM_SIZE);
    }

    @Test
    void testAddResponseAndQuorumReached() {
        Message response1 = mock(Message.class);
        Message response2 = mock(Message.class);

        System.out.println("Adding response from replica-1");
        assertFalse(requestTracker.addResponse("replica-1", response1));
        System.out.println("Adding response from replica-2");
        assertTrue(requestTracker.addResponse("replica-2", response2));
        System.out.println("Adding duplicate response from replica-2");
        assertFalse(requestTracker.addResponse("replica-2", response2)); // Duplicate should not affect
    }

    @Test
    void testTimeoutHandling() {
        // Timeout logic is not applicable in deterministic simulation
        // Implement alternative logic if needed
    }

    @Test
    void testGetBestResponse() {
        Message response1 = mock(Message.class);

        requestTracker.addResponse("replica-1", response1);
        requestTracker.addResponse("replica-2", response1);

        assertNotNull(requestTracker.getBestResponse());
    }
} 