package com.dststore.replica;

import com.dststore.network.InetAddressAndPort;

/**
 * Represents a network endpoint for a replica node in the distributed system.
 * Contains the network location information (IP address and port) necessary
 * for other nodes to communicate with the replica.
 */
public class ReplicaEndpoint {
    private final String replicaId;
    private final InetAddressAndPort address;
    
    public ReplicaEndpoint(String replicaId, InetAddressAndPort address) {
        if (replicaId == null || replicaId.isEmpty()) {
            throw new IllegalArgumentException("Replica ID cannot be null or empty");
        }
        if (address == null) {
            throw new IllegalArgumentException("Address cannot be null");
        }
        
        this.replicaId = replicaId;
        this.address = address;
    }
    
    // Constructor for backward compatibility
    public ReplicaEndpoint(String replicaId, String ipAddress, int port) {
        this(replicaId, new InetAddressAndPort(ipAddress, port));
    }
    
    public String getReplicaId() {
        return replicaId;
    }
    
    public InetAddressAndPort getAddress() {
        return address;
    }
    
    // For backward compatibility
    public String getIpAddress() {
        return address.getIpAddress();
    }
    
    // For backward compatibility
    public int getPort() {
        return address.getPort();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        ReplicaEndpoint that = (ReplicaEndpoint) o;
        return replicaId.equals(that.replicaId);
    }
    
    @Override
    public int hashCode() {
        return replicaId.hashCode();
    }
    
    @Override
    public String toString() {
        return "ReplicaEndpoint{" +
               "replicaId='" + replicaId + '\'' +
               ", address=" + address +
               '}';
    }
} 