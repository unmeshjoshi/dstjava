package com.dststore.network;

import java.util.Arrays;
import java.util.UUID;

/**
 * Represents a network packet traveling between nodes.
 * A packet contains serialized message data and metadata for routing and simulation.
 */
public class Packet {
    private final UUID packetId;
    private final Path path;
    private final byte[] payload;
    private final long createdAtTick;
    
    /**
     * Creates a new packet with the given properties.
     *
     * @param path The network path this packet should travel
     * @param payload The serialized message data
     * @param createdAtTick The simulation tick when this packet was created
     */
    public Packet(Path path, byte[] payload, long createdAtTick) {
        this.packetId = UUID.randomUUID();
        this.path = path;
        this.payload = Arrays.copyOf(payload, payload.length);
        this.createdAtTick = createdAtTick;
    }
    
    /**
     * Creates a new packet with full control over all properties.
     * Primarily used for testing or when exact reproducibility is needed.
     *
     * @param packetId The unique identifier for this packet
     * @param path The network path this packet should travel
     * @param payload The serialized message data
     * @param createdAtTick The simulation tick when this packet was created
     */
    public Packet(UUID packetId, Path path, byte[] payload, long createdAtTick) {
        this.packetId = packetId;
        this.path = path;
        this.payload = Arrays.copyOf(payload, payload.length);
        this.createdAtTick = createdAtTick;
    }
    
    /**
     * Gets the unique identifier for this packet.
     *
     * @return The packet's UUID
     */
    public UUID getPacketId() {
        return packetId;
    }
    
    /**
     * Gets the network path this packet is traveling.
     *
     * @return The path object
     */
    public Path getPath() {
        return path;
    }
    
    /**
     * Gets the serialized message payload.
     * Returns a defensive copy to prevent modification.
     *
     * @return A copy of the payload data
     */
    public byte[] getPayload() {
        return Arrays.copyOf(payload, payload.length);
    }
    
    /**
     * Gets the simulation tick when this packet was created.
     *
     * @return The creation tick
     */
    public long getCreatedAtTick() {
        return createdAtTick;
    }
    
    /**
     * Creates a new packet as a response to this packet.
     * The path is reversed, and a new payload and creation tick are used.
     *
     * @param responsePayload The payload for the response
     * @param currentTick The current simulation tick
     * @return A new packet representing the response
     */
    public Packet createResponse(byte[] responsePayload, long currentTick) {
        return new Packet(path.reverse(), responsePayload, currentTick);
    }
    
    @Override
    public String toString() {
        return "Packet{" +
                "id=" + packetId +
                ", path=" + path +
                ", payloadSize=" + payload.length +
                ", createdAt=" + createdAtTick +
                '}';
    }
} 