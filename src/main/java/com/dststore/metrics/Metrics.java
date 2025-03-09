package com.dststore.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Collects and stores metrics for the system.
 */
public class Metrics {
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong successfulRequests = new AtomicLong(0);
    private final AtomicLong failedRequests = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> latencyMetrics = new ConcurrentHashMap<>();

    /**
     * Records a request.
     */
    public void recordRequest() {
        totalRequests.incrementAndGet();
    }

    /**
     * Records a successful request.
     */
    public void recordSuccessfulRequest() {
        successfulRequests.incrementAndGet();
    }

    /**
     * Records a failed request.
     */
    public void recordFailedRequest() {
        failedRequests.incrementAndGet();
    }

    /**
     * Records the latency for a request.
     *
     * @param requestId The ID of the request
     * @param latency The latency in milliseconds
     */
    public void recordLatency(String requestId, long latency) {
        latencyMetrics.put(requestId, new AtomicLong(latency));
    }

    /**
     * Gets the total number of requests.
     *
     * @return The total number of requests
     */
    public long getTotalRequests() {
        return totalRequests.get();
    }

    /**
     * Gets the total number of successful requests.
     *
     * @return The total number of successful requests
     */
    public long getSuccessfulRequests() {
        return successfulRequests.get();
    }

    /**
     * Gets the total number of failed requests.
     *
     * @return The total number of failed requests
     */
    public long getFailedRequests() {
        return failedRequests.get();
    }

    /**
     * Gets the average latency of requests.
     *
     * @return The average latency in milliseconds
     */
    public double getAverageLatency() {
        return latencyMetrics.values().stream().mapToLong(AtomicLong::get).average().orElse(0.0);
    }

    /**
     * Generates a report of the current metrics.
     *
     * @return A string report of the metrics
     */
    public String generateReport() {
        return String.format("Total Requests: %d, Successful Requests: %d, Failed Requests: %d, Average Latency: %.2f ms",
                getTotalRequests(), getSuccessfulRequests(), getFailedRequests(), getAverageLatency());
    }
} 