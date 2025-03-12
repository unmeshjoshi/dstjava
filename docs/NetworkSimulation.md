# Network Simulation for Distributed Systems Testing

This document explains how to use the network simulation capabilities in the distributed store (dststore) to test your distributed system under various network conditions in a controlled, deterministic manner.

## Overview

Testing distributed systems is challenging because many failures and bugs only manifest under specific network conditions that are difficult to reproduce consistently. The `NetworkSimulator` provides a way to create deterministic, reproducible tests by controlling:

- **Network partitions**: Isolate groups of nodes from communicating with each other
- **Message loss**: Simulate packet loss at configurable rates
- **Message delays**: Add latency to message delivery
- **Bandwidth constraints**: Limit how many messages can be processed per time unit
- **Custom message filters**: Apply custom logic to affect specific messages

The simulator uses a tick-based approach to time, instead of wall-clock time, making tests deterministic and reproducible regardless of system load or execution environment.

## Getting Started

### Basic Setup

To use the network simulator in your tests:

```java
// Create a message bus with network simulation enabled
MessageBus messageBus = new MessageBus();
NetworkSimulator simulator = messageBus.enableNetworkSimulation();

// Create your distributed system components
Replica replica1 = new Replica("replica-1", messageBus, ...);
Replica replica2 = new Replica("replica-2", messageBus, ...);
Client client = new Client("client-1", messageBus, ...);

// Configure network conditions as needed
simulator.withMessageLossRate(0.1)   // 10% packet loss
         .withLatency(2, 5);         // 2-5 tick delay

// Run your test with ticks
for (int i = 0; i < 100; i++) {
    messageBus.tick();  // Advance the simulation time
    replica1.tick();    // Let components process messages
    replica2.tick();
    client.tick();
    
    // Test assertions at various points
}
```

### Running the Simulation

The simulation advances in discrete time steps called "ticks". During each tick:

1. The `MessageBus` processes any delayed messages that are now ready for delivery
2. Each component processes any incoming messages in its queue
3. Components may send new messages which get processed by the network simulator

It's important to call the `tick()` method on all components (MessageBus, Replicas, Clients, etc.) for each simulation tick to ensure proper message processing.

## Simulating Network Conditions

### Network Partitions

Network partitions occur when groups of nodes can't communicate with each other, often due to network failures. To simulate partitions:

```java
// Create partitions
simulator.createPartition("replica-1");              // Partition 0
simulator.createPartition("replica-2", "replica-3"); // Partition 1
simulator.createPartition("client-1");               // Partition 2

// Allow specific partitions to communicate
simulator.linkPartitions(0, 2);  // Link partition 0 and 2
```

This setup creates three separate partitions where nodes can only communicate within their partition or with linked partitions. Messages sent between unlinked partitions will be dropped.

To heal partitions, either link them or reset the simulator:

```java
// Link all partitions
simulator.linkPartitions(0, 1);
simulator.linkPartitions(1, 2);

// Or reset all network conditions
simulator.reset();
```

### Message Loss

Message loss simulates packets being dropped in the network:

```java
// Set a 30% message loss rate
simulator.withMessageLossRate(0.3);

// Disable message loss
simulator.withMessageLossRate(0.0);
```

### Message Latency

Message latency simulates network delays:

```java
// Set a fixed latency of 5 ticks
simulator.withLatency(5, 5);

// Set a variable latency between 2 and 8 ticks
simulator.withLatency(2, 8);

// Disable latency
simulator.withLatency(0, 0);
```

### Bandwidth Limitations

Bandwidth limitations restrict how many messages can be delivered per tick:

```java
// Limit to 5 messages per tick
simulator.withBandwidthLimit(5);

// Remove bandwidth limitations
simulator.withBandwidthLimit(Integer.MAX_VALUE);
```

### Custom Message Filters

For more complex scenarios, you can create custom message filters:

```java
// Filter that drops all PUT requests
simulator.withMessageFilter((message, from, to) -> {
    if (message instanceof PutRequest) {
        return true;  // Apply filter (drop the message)
    }
    return false;  // Don't apply filter
});
```

## Testing Strategies

Here are some effective strategies for testing distributed systems with the network simulator:

### 1. Test Normal Operations

Start with tests that verify your system works correctly under normal network conditions:

```java
// Configure normal network conditions
simulator.withMessageLossRate(0.0).withLatency(0, 0);

// Run your test operations
CompletableFuture<Response> future = client.sendRequest(...);
runSimulation(10);  // Run for 10 ticks

// Verify results
assertTrue(future.isDone());
assertTrue(future.get().isSuccess());
```

### 2. Test Message Loss Resilience

Verify your system handles message loss correctly:

```java
// Configure high message loss
simulator.withMessageLossRate(0.5);  // 50% loss rate

// Run your test operations
CompletableFuture<Response> future = client.sendRequest(...);
runSimulation(30);  // Run for more ticks to account for retries

// Verify results
assertTrue(future.isDone());
// Note: Depending on your system, the request might succeed or fail
```

### 3. Test Network Partitions

Verify your system handles network partitions correctly:

```java
// Create a partition that separates replicas
simulator.createPartition("replica-1");
simulator.createPartition("replica-2", "replica-3");
simulator.createPartition("client-1");
simulator.linkPartitions(0, 2);  // Client can talk to replica-1

// Run your test operations 
CompletableFuture<Response> future = client.sendRequest(...);
runSimulation(20);

// Verify results - should detect lack of quorum
assertTrue(future.isDone());
assertFalse(future.get().isSuccess());
```

### 4. Test Self-Healing

Verify your system recovers when network issues are resolved:

```java
// Start with a network partition
simulator.createPartition("replica-1");
simulator.createPartition("replica-2", "replica-3");
simulator.createPartition("client-1");

// Make a request that won't complete due to partition
CompletableFuture<Response> future = client.sendRequest(...);
runSimulation(5);

// Heal the network
simulator.reset();

// Verify the system recovers
runSimulation(15);
assertTrue(future.isDone());
// Check the response based on your system's expected behavior
```

## Best Practices

1. **Start Simple**: Begin with tests of normal operation before adding network complexities
2. **Isolate Variables**: Test one network condition at a time to understand its impact
3. **Test Edge Cases**: Focus on boundary conditions like just-missed quorums or timeouts
4. **Use Determinism**: Take advantage of the deterministic nature by using fixed seeds
5. **Log Extensively**: Enable detailed logging to trace message flows and diagnoses issues
6. **Vary Tick Counts**: Some issues only appear after many ticks or with specific timing
7. **Combine Conditions**: Once basic tests pass, combine conditions (e.g., partitions with message loss)

## Common Pitfalls

1. **Forgetting to Tick**: Ensure all components call `tick()` at each time step
2. **Unrealistic Conditions**: Extreme conditions (100% loss, huge partitions) may not be realistic
3. **Inadequate Timeouts**: Configure appropriate timeout thresholds for your simulation ticks
4. **Non-Deterministic Elements**: Avoid `System.currentTimeMillis()` or other non-deterministic operations
5. **Incomplete Recovery**: Test that recovery is complete after network healing

## Debugging Tips

1. **Check Simulator Stats**: Use `simulator.getStats()` to verify configuration
2. **Trace Messages**: Enable DEBUG logging to see message flow
3. **Isolate Components**: Test with minimal nodes to pinpoint issues
4. **Verify Partitions**: Use `simulator.canCommunicate(from, to)` to verify partition setup
5. **Step Through**: Run a few ticks at a time and check state between ticks

## Examples

See the `NetworkSimulationTest.java` class for comprehensive examples of testing with the network simulator.

## Conclusion

The network simulator provides powerful capabilities for testing distributed systems under controlled network conditions. By using this approach, you can find and fix subtle bugs that would be difficult to reproduce in real-world environments, making your distributed system more robust and reliable. 