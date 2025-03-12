package com.dststore.network;

import java.util.Objects;

/**
 * Encapsulates network address information with IP address and port.
 * Used to represent network endpoints in the distributed system.
 */
public class InetAddressAndPort {
    private final String ipAddress;
    private final int port;
    
    public InetAddressAndPort(String ipAddress, int port) {
        if (ipAddress == null || ipAddress.isEmpty()) {
            throw new IllegalArgumentException("IP address cannot be null or empty");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("Port must be a positive integer");
        }
        
        this.ipAddress = ipAddress;
        this.port = port;
    }
    
    public String getIpAddress() {
        return ipAddress;
    }
    
    public int getPort() {
        return port;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        InetAddressAndPort that = (InetAddressAndPort) o;
        return port == that.port && 
               Objects.equals(ipAddress, that.ipAddress);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(ipAddress, port);
    }
    
    @Override
    public String toString() {
        return ipAddress + ":" + port;
    }
} 