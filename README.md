# DST Store - Deterministic Simulation Testing for Distributed Key-Value Store

This project implements a replicated key-value store with deterministic simulation testing inspired by TigerBeetle's approach. The core idea is to create a system that simulates network conditions in a controlled, reproducible way to test distributed systems behavior.

## Key Features

- **Tick-based simulation**: All components advance time in discrete "ticks" rather than wall-clock time, allowing controlled and reproducible execution.
- **Deterministic PRNG**: Random events (packet loss, delays) are generated using seeded PRNGs to ensure reproducibility.
- **Simulated network conditions**: Packet loss, delays, reordering, and network partitions can be precisely simulated.
- **Controlled message delivery**: Messages between replicas are delivered according to simulation rules rather than actual network behavior.
- **Quorum-based replication**: Requests are sent to all replicas and responses from a quorum are required for success.

## Project Structure

The project is organized into several key components:

- **Message**: Base classes for communication between replicas and clients
- **Network**: Simulation of network conditions and packet delivery
- **Storage**: Local key-value storage implementation
- **Replica**: Implementation of the replicated nodes
- **Simulation**: Framework for running deterministic simulations

## Building the Project

This project uses Gradle for dependency management and building:

```bash
# Build the project
./gradlew build

# Run tests
./gradlew test

# Run the application
./gradlew run
```

## Implementation Progress

- [x] Basic Message Structure with JSON serialization
- [x] Network Path and Packet
- [x] Packet Simulator
- [x] Simulated Clock
- [x] Message Bus
- [x] Key-Value Store
- [x] Basic Replica Implementation
- [x] Request Handling
- [ ] Request Tracker for Quorum
- [x] Simulated Environment
- [x] Implement GET operation with potential consistency handling
  - Complete the GET request/response cycle
  - Handle potential inconsistencies between replicas
  - Implement conflict resolution if needed
  - Test: Verify GET operation end-to-end.
- [ ] Network Partitioning
- [ ] Metrics Collection
- [ ] Conflict Resolution
- [ ] Client Library

## License

This project is licensed under the MIT License - see the LICENSE file for details. 