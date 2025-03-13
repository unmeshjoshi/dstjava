package com.dststore.quorum;

import com.dststore.quorum.messages.GetValueResponse;
import com.dststore.quorum.messages.SetValueResponse;

import java.util.List;

/**
 * Callback interface for quorum operations.
 * Provides methods to handle the completion of quorum operations.
 */
public interface QuorumCallback {
    /**
     * Called when a GET operation has reached quorum.
     *
     * @param key The key that was queried
     * @param responses The responses received from replicas
     * @param clientMessageId The original client message ID
     */
    void onGetQuorumReached(String key, List<GetValueResponse> responses, String clientMessageId);

    /**
     * Called when a SET operation has reached quorum.
     *
     * @param key The key that was set
     * @param value The value that was set
     * @param responses The responses received from replicas
     * @param clientMessageId The original client message ID
     */
    void onSetQuorumReached(String key, String value, List<SetValueResponse> responses, String clientMessageId);

    /**
     * Called when a GET operation has timed out before reaching quorum.
     *
     * @param key The key that was queried
     * @param responses The responses received so far
     * @param clientMessageId The original client message ID
     */
    void onGetQuorumTimeout(String key, List<GetValueResponse> responses, String clientMessageId);

    /**
     * Called when a SET operation has timed out before reaching quorum.
     *
     * @param key The key that was being set
     * @param value The value that was being set
     * @param responses The responses received so far
     * @param clientMessageId The original client message ID
     */
    void onSetQuorumTimeout(String key, String value, List<SetValueResponse> responses, String clientMessageId);
} 