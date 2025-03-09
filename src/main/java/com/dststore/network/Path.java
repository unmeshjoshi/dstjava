package com.dststore.network;

import java.util.Objects;

/**
 * Represents a directional network path between two nodes.
 * A path is defined by its source and target node identifiers.
 */
public class Path {
    private final String sourceId;
    private final String targetId;

    /**
     * Creates a new path between source and target nodes.
     *
     * @param sourceId The identifier of the source node
     * @param targetId The identifier of the target node
     */
    public Path(String sourceId, String targetId) {
        this.sourceId = sourceId;
        this.targetId = targetId;
    }

    /**
     * Gets the source node identifier.
     *
     * @return The source node identifier
     */
    public String getSourceId() {
        return sourceId;
    }

    /**
     * Gets the target node identifier.
     *
     * @return The target node identifier
     */
    public String getTargetId() {
        return targetId;
    }

    /**
     * Creates a reverse path (target -> source).
     *
     * @return A new Path object representing the reverse direction
     */
    public Path reverse() {
        return new Path(targetId, sourceId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Path path = (Path) o;
        return Objects.equals(sourceId, path.sourceId) && 
               Objects.equals(targetId, path.targetId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceId, targetId);
    }

    @Override
    public String toString() {
        return sourceId + " -> " + targetId;
    }
} 