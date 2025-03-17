package com.dststore.network;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents network partitions in a distributed system simulation.
 * A partition is a group of nodes that can communicate with each other.
 * Communication between partitions is controlled through explicit links.
 */
public class NetworkPartition {
    private static final Logger LOGGER = Logger.getLogger(NetworkPartition.class.getName());

    // Maps partition IDs to the set of nodes in that partition
    private final List<Set<String>> partitions = new ArrayList<>();
    
    // Maps source partition ID to the set of target partition IDs it can communicate with
    private final Map<Integer, Set<Integer>> partitionLinks = new HashMap<>();

    /**
     * Creates a new partition containing the specified nodes.
     *
     * @param nodeIds The IDs of nodes to include in the partition
     * @return The ID of the newly created partition
     */
    public int createPartition(String... nodeIds) {
        if (nodeIds == null || nodeIds.length == 0) {
            throw new IllegalArgumentException("Node IDs cannot be null or empty");
        }

        // Check if any node is already in another partition
        for (String nodeId : nodeIds) {
            for (Set<String> partition : partitions) {
                if (partition.contains(nodeId)) {
                    throw new IllegalArgumentException(
                        String.format("Node %s is already in another partition", nodeId));
                }
            }
        }

        Set<String> partition = new HashSet<>(Arrays.asList(nodeIds));
        partitions.add(partition);
        int partitionId = partitions.size() - 1;
        
        LOGGER.log(Level.INFO, "Created partition {0} with nodes: {1}", 
                  new Object[]{partitionId, Arrays.toString(nodeIds)});
        
        return partitionId;
    }

    /**
     * Creates a link allowing communication from the source partition to the target partition.
     *
     * @param sourcePartitionId ID of the source partition
     * @param targetPartitionId ID of the target partition
     */
    public void linkPartitions(int sourcePartitionId, int targetPartitionId) {
        validatePartitionId(sourcePartitionId);
        validatePartitionId(targetPartitionId);

        partitionLinks.computeIfAbsent(sourcePartitionId, k -> new HashSet<>())
                     .add(targetPartitionId);
        
        LOGGER.log(Level.INFO, "Created link from partition {0} to partition {1}", 
                  new Object[]{sourcePartitionId, targetPartitionId});
    }

    /**
     * Creates bidirectional links between two partitions.
     *
     * @param partition1Id First partition ID
     * @param partition2Id Second partition ID
     */
    public void linkPartitionsBidirectional(int partition1Id, int partition2Id) {
        linkPartitions(partition1Id, partition2Id);
        linkPartitions(partition2Id, partition1Id);
    }

    /**
     * Checks if two nodes can communicate based on their partition membership and links.
     *
     * @param from Source node ID
     * @param to Target node ID
     * @return true if the nodes can communicate, false otherwise
     */
    public boolean canCommunicate(String from, String to) {
        // If either node isn't in any partition, they can communicate
        int fromPartition = findPartitionForNode(from);
        int toPartition = findPartitionForNode(to);

        if (fromPartition == -1 || toPartition == -1) {
            return true;
        }

        // If they're in the same partition, they can communicate
        if (fromPartition == toPartition) {
            return true;
        }

        // Check if there's a link between the partitions
        Set<Integer> links = partitionLinks.get(fromPartition);
        return links != null && links.contains(toPartition);
    }

    /**
     * Finds which partition contains the specified node.
     *
     * @param nodeId The node ID to look for
     * @return The partition ID containing the node, or -1 if not found
     */
    private int findPartitionForNode(String nodeId) {
        for (int i = 0; i < partitions.size(); i++) {
            if (partitions.get(i).contains(nodeId)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Validates that a partition ID exists.
     *
     * @param partitionId The partition ID to validate
     * @throws IllegalArgumentException if the partition ID is invalid
     */
    private void validatePartitionId(int partitionId) {
        if (partitionId < 0 || partitionId >= partitions.size()) {
            throw new IllegalArgumentException(
                String.format("Invalid partition ID: %d", partitionId));
        }
    }

    /**
     * Clears all partitions and links.
     */
    public void reset() {
        partitions.clear();
        partitionLinks.clear();
        LOGGER.info("Reset all partitions and links");
    }

    /**
     * Gets a map of statistics about the current partitioning.
     *
     * @return Map containing partition statistics
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("partitionCount", partitions.size());
        stats.put("totalNodes", partitions.stream()
                                        .mapToInt(Set::size)
                                        .sum());
        stats.put("totalLinks", partitionLinks.values().stream()
                                            .mapToInt(Set::size)
                                            .sum());
        return stats;
    }

    /**
     * Gets the total number of partitions.
     *
     * @return The number of partitions
     */
    public int getPartitionCount() {
        return partitions.size();
    }

    /**
     * Gets the total number of links between partitions.
     *
     * @return The number of partition links
     */
    public int getPartitionLinkCount() {
        return partitionLinks.values().stream()
                           .mapToInt(Set::size)
                           .sum();
    }
} 