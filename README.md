# Deterministic Testing Framework for Distributed Systems

This project provides a framework for deterministic testing of distributed systems, inspired by TigerBeetle's approach to testing distributed systems. The framework allows for controlled simulation of network conditions, node failures, and message delivery, making it possible to write reproducible tests for distributed systems.

## Key Components

### NetworkSimulator

The `NetworkSimulator` class provides the ability to simulate various network conditions:

- **Network Partitions**: Create partitions that completely separate groups of nodes
- **Message Loss**: Configure a rate at which messages are randomly dropped
- **Message Delays**: Add latency to message delivery
- **Bandwidth Limitations**: Restrict the number of messages that can be processed per tick
- **Custom Message Filtering**: Apply custom logic to filter or modify messages

### SimulationRunner

The `SimulationRunner` class orchestrates the simulation:

- **Tick-based Simulation**: Advances the simulation one tick at a time
- **Replica Management**: Controls the lifecycle of replicas (start/stop)
- **Network Control**: Creates and heals network partitions
- **Condition-based Execution**: Runs the simulation until a specific condition is met

## Writing Deterministic Tests

The framework enables writing tests that are:

1. **Deterministic**: Tests produce the same results every time they run
2. **Controlled**: Network conditions and timing are precisely managed
3. **Comprehensive**: Tests can cover edge cases that are difficult to reproduce in real environments

Example test cases include:

- Basic replication behavior
- Network partition scenarios
- Node failure and recovery
- Message loss and delays
- Complex scenarios with multiple failures

## Example Test

Here's a simple example of a test using the framework:

```java
@Test
public void testNetworkPartition() throws Exception {
    // Create a network partition
    int[] partitions = simulation.createNetworkPartition(
        new String[]{"replica-1", "client-1"}, 
        new String[]{"replica-2", "replica-3"});
    
    // Put a value to replica-1
    CompletableFuture<PutResponse> putFuture = client.put("key", "value", "replica-1");
    
    // Run the simulation for 10 ticks
    simulation.runFor(10);
    
    // Check that the put failed (can't achieve quorum)
    assertTrue(putFuture.isDone());
    assertFalse(putFuture.get().isSuccess());
    
    // Heal the partition
    simulation.healNetworkPartition(partitions[0], partitions[1]);
    
    // Try the put again
    putFuture = client.put("key", "value", "replica-1");
    
    // Run the simulation for more ticks
    simulation.runFor(10);
    
    // Check that the put succeeded
    assertTrue(putFuture.isDone());
    assertTrue(putFuture.get().isSuccess());
}
```

## Benefits of Deterministic Testing

1. **Reproducibility**: Tests consistently produce the same results
2. **Debuggability**: Failures can be analyzed step by step
3. **Comprehensive Coverage**: Edge cases can be systematically tested
4. **Fast Execution**: Tests run faster than real-time simulations
5. **No External Dependencies**: Tests don't require actual network infrastructure

## Getting Started

To use the framework in your tests:

1. Create a `MessageBus` instance
2. Create your replicas and register them with the message bus
3. Create a `SimulationRunner` with your replicas and message bus
4. Write tests that use the simulation runner to control the environment
5. Run your tests and analyze the results

For detailed examples, see the `DeterministicReplicationTest` class.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 