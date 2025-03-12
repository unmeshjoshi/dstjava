package com.dststore.replica;

public class Timeout {
    private final long startTick;
    private final long timeoutTicks;
    private boolean expired = false;
    
    public Timeout(long startTick, long timeoutTicks) {
        if (timeoutTicks <= 0) {
            throw new IllegalArgumentException("Timeout ticks must be positive");
        }
        
        this.startTick = startTick;
        this.timeoutTicks = timeoutTicks;
    }
    
    public boolean isExpired(long currentTick) {
        if (!expired) {
            expired = (currentTick - startTick) >= timeoutTicks;
        }
        return expired;
    }
    
    public long getStartTick() {
        return startTick;
    }
    
    public long getTimeoutTicks() {
        return timeoutTicks;
    }
    
    public long getExpiryTick() {
        return startTick + timeoutTicks;
    }
    
    public long getTicksRemaining(long currentTick) {
        if (isExpired(currentTick)) {
            return 0;
        }
        return getExpiryTick() - currentTick;
    }
    
    @Override
    public String toString() {
        return "Timeout{startTick=" + startTick + 
               ", timeoutTicks=" + timeoutTicks + 
               ", expiryTick=" + getExpiryTick() + 
               ", expired=" + expired + "}";
    }
} 