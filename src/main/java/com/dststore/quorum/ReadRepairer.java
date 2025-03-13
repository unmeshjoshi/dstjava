package com.dststore.quorum;

import com.dststore.network.MessageBus;
import com.dststore.quorum.messages.GetValueResponse;
import com.dststore.quorum.messages.VersionedSetValueRequest;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Handles read repair operations for the quorum-based key-value store.
 * Read repair ensures that all replicas eventually have the most up-to-date value.
 */
public class ReadRepairer {
    private static final Logger LOGGER = Logger.getLogger(ReadRepairer.class.getName());
    private final MessageBus messageBus;
    private final String replicaId;

    public ReadRepairer(MessageBus messageBus, String replicaId) {
        this.messageBus = messageBus;
        this.replicaId = replicaId;
    }

    /**
     * Performs read repair based on the responses received from replicas.
     * If inconsistencies are detected, updates the outdated replicas.
     *
     * @param responses The responses received from replicas
     * @param key The key that was queried
     */
    public void performReadRepair(List<GetValueResponse> responses, String key) {
        if (responses.isEmpty()) {
            LOGGER.warning("No responses to perform read repair for key: " + key);
            return;
        }

        // Find the response with the highest version
        GetValueResponse latestResponse = findLatestResponse(responses);
        
        if (latestResponse == null) {
            LOGGER.warning("No successful responses to perform read repair for key: " + key);
            return;
        }

        // Identify replicas with outdated values
        List<String> outdatedReplicas = responses.stream()
                .filter(r -> r.isSuccess() && r.getVersion() < latestResponse.getVersion())
                .map(GetValueResponse::getReplicaId)
                .collect(Collectors.toList());

        if (outdatedReplicas.isEmpty()) {
            LOGGER.fine("No outdated replicas found for key: " + key);
            return;
        }

        // Send versioned set requests to outdated replicas
        for (String outdatedReplicaId : outdatedReplicas) {
            String messageId = UUID.randomUUID().toString();
            VersionedSetValueRequest repairRequest = new VersionedSetValueRequest(
                    messageId,
                    key,
                    latestResponse.getValue(),
                    replicaId,
                    latestResponse.getVersion()
            );

            messageBus.send(outdatedReplicaId, replicaId, repairRequest);
            LOGGER.info("Sent read repair for key " + key + " to replica " + outdatedReplicaId + 
                       " with version " + latestResponse.getVersion());
        }
    }

    /**
     * Finds the response with the highest version among successful responses.
     *
     * @param responses The list of responses to check
     * @return The response with the highest version, or null if no successful responses
     */
    private GetValueResponse findLatestResponse(List<GetValueResponse> responses) {
        return responses.stream()
                .filter(GetValueResponse::isSuccess)
                .max((r1, r2) -> Long.compare(r1.getVersion(), r2.getVersion()))
                .orElse(null);
    }
} 