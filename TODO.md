TODO.md - Replicated Key-Value Store with Deterministic Simulation Testing
Context for Implementation
This project implements a replicated key-value store with deterministic simulation testing inspired by TigerBeetle's approach. The core idea is to create a system that simulates network conditions in a controlled, reproducible way to test distributed systems behavior.
Key aspects of deterministic simulation testing:
Tick-based simulation: All components advance time in discrete "ticks" rather than wall-clock time, allowing controlled and reproducible execution.
Deterministic PRNG: Random events (packet loss, delays) are generated using seeded PRNGs to ensure reproducibility.
Simulated network conditions: Packet loss, delays, reordering, and network partitions can be precisely simulated.
Controlled message delivery: Messages between replicas are delivered according to simulation rules rather than actual network behavior.
Quorum-based replication: Requests are sent to all replicas and responses from a quorum are required for success.
No consensus algorithm: Unlike TigerBeetle's VSR, we implement a simpler replication scheme where clients send requests to all replicas and wait for a quorum of responses.
The implementation follows a tick-based pattern where each component has a tick() method that advances its state by one time unit. This approach allows us to control the timing of events in the simulation and create reproducible test scenarios.
Implementation Steps
1. Basic Message Structure
Task: Implement the basic Message class hierarchy with JSON serialization.
Implementation Notes:
Create a base Message class with common fields (messageId, type, etc.)
Implement concrete message types: SetRequest, SetResponse, GetRequest, GetResponse
Add Jackson annotations for proper JSON serialization/deserialization
Implement type discriminator for polymorphic deserialization
Test: Verify message creation and JSON serialization/deserialization.
2. Network Path and Packet
Task: Implement Path and Packet classes for network simulation.
Implementation Notes:
Path: Represents a connection between replicas (source -> target)
Packet: Wrapper for serialized message data being transmitted over network
Keep these simple - Path just contains source/target IDs, Packet just contains bytes
Test: Verify Path equality and Packet creation.
3. Packet Simulator
Task: Implement the core PacketSimulator for deterministic network simulation.
Implementation Notes:
Create PacketSimulatorOptions with parameters for network conditions (delay, loss probability, etc.)
Implement PacketSimulator with a tick-based design for deterministic delivery
Use a priority queue to schedule packet delivery
Implement packet loss and delay using seeded PRNG
Maintain statistics for dropped/delivered packets
Test: Verify deterministic packet delivery with controlled timing.
4. Simulated Clock
Task: Implement a SimulatedClock to provide deterministic timing.
Implementation Notes:
Simple counter-based clock that advances in discrete ticks
No dependency on wall-clock time
Methods for advancing time in single or multiple ticks
Test: Verify clock advances correctly.
5. Message Bus
Task: Implement the MessageBus for replica communication.
Implementation Notes:
Uses PacketSimulator for message delivery
Handles serialization/deserialization between Message objects and Packets
Maintains connections between replicas
Implements tick() method to process incoming/outgoing messages
Callback mechanism for delivering received messages to Replica
Test: Verify message delivery between simulated replicas.
6. Key-Value Store
Task: Implement the local storage for key-value pairs.
Implementation Notes:
In-memory storage with disk persistence
Basic get/set operations
JSON-based storage format
Recovery from disk on startup
Simple timestamp-based conflict resolution
Test: Verify basic storage operations and persistence.
7. Basic Replica Implementation
Task: Implement the core Replica class with tick method.
Implementation Notes:
Manages local storage
Processes incoming messages
Sends outgoing messages via MessageBus
Implements tick() method to advance state
Tracks statistics and state information
Test: Verify basic replica operation with tick advancement.
8. Request Handling
Task: Implement request processing logic in Replica.
Implementation Notes:
Methods to handle client requests
Logic to forward requests to other replicas
Processing of replica responses
Integration with local storage
Test: Verify request handling and forwarding.
9. Request Tracker for Quorum
Task: Implement RequestTracker for quorum response management.
Implementation Notes:
Tracks responses for a given request
Determines when quorum is reached
Selects appropriate response to return to client
Handles timeout for incomplete requests
Test: Verify quorum calculation and response selection.
10. Simulated Environment
Task: Create a SimulatedEnvironment for end-to-end testing.
Implementation Notes:
Orchestrates multiple replicas in a simulated network
Controls network conditions
Provides client interface for sending requests
Methods for creating failures and partitions
Utilities to run simulation for specified ticks
Test: Verify end-to-end operation with controlled failures.
11. GET Operation
- [x] Task: Implement the GET operation with potential consistency handling.
  - Complete the GET request/response cycle
  - Handle potential inconsistencies between replicas
  - Implement conflict resolution if needed
  - Test: Verify GET operation end-to-end.
12. Network Partitioning
Task: Add support for simulating network partitions.
Implementation Notes:
Enhance PacketSimulator to support partitioning
Add partition management methods to SimulatedEnvironment
Support partition healing
Test: Verify partition behavior impacts quorum operations.
13. Metrics Collection
Task: Add metrics collection and reporting.
Implementation Notes:
Track request latency, throughput, etc.
Generate statistics for analysis
Support logging for debugging
Test: Verify metrics collection during simulation.
14. Conflict Resolution
Task: Implement timestamp-based conflict resolution.
Implementation Notes:
Add timestamps to SET operations
Logic to resolve conflicts based on timestamps
Ensure consistent state across replicas
Test: Verify conflict resolution with concurrent writes.
15. Client Library
Task: Create a simple client library for the key-value store.
Implementation Notes:
Clean API for get/set operations
Handle communication with replicas
Implement retry logic
Support request timeouts
Test: Verify client library operation.
Starting Prompt for Implementation
When starting a new chat session to implement this project, begin with:
"I'm implementing a replicated key-value store with deterministic simulation testing inspired by TigerBeetle. The core of this approach is a tick-based simulation system where all components advance time in discrete steps, allowing for controlled and reproducible testing of distributed systems.
Let's start implementing this step by step, following TDD principles. The first component to implement is the basic Message structure with JSON serialization support. This will be the foundation for our communication protocol between replicas and clients."
