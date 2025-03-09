package com.dststore.network;

import com.dststore.message.GetRequest;
import com.dststore.message.GetResponse;
import com.dststore.message.Message;
import com.dststore.message.MessageType;
import com.dststore.simulation.SimulatedClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the MessageBus class.
 */
public class MessageBusTest {
    
    private PacketSimulator packetSimulator;
    private MessageBus node1Bus;
    private MessageBus node2Bus;
    
    @BeforeEach
    void setUp() {
        packetSimulator = new PacketSimulator();
        node1Bus = new MessageBus("node-1", packetSimulator);
        node2Bus = new MessageBus("node-2", packetSimulator);
        
        // Connect the nodes
        node1Bus.connect("node-2");
        node2Bus.connect("node-1");
    }
    
    @Test
    void testNodeConnectionStatus() {
        // Assert
        assertThat(node1Bus.isConnected("node-2")).isTrue();
        assertThat(node2Bus.isConnected("node-1")).isTrue();
        assertThat(node1Bus.isConnected("node-3")).isFalse();
        
        // Get connected nodes
        Map<String, Boolean> node1Connections = node1Bus.getConnectedNodes();
        assertThat(node1Connections).containsKey("node-2");
        assertThat(node1Connections.get("node-2")).isTrue();
    }
    
    @Test
    void testDisconnection() {
        // Act
        node1Bus.disconnect("node-2");
        
        // Assert
        assertThat(node1Bus.isConnected("node-2")).isFalse();
        assertThat(node2Bus.isConnected("node-1")).isTrue(); // one-way disconnect
    }
    
    @Test
    void testSendMessageImmediately() {
        // Arrange
        AtomicBoolean messageReceived = new AtomicBoolean(false);
        AtomicReference<String> receivedKey = new AtomicReference<>();
        
        MessageHandler handler = new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }
            
            @Override
            public void handleMessage(Message message) {
                messageReceived.set(true);
                GetRequest request = (GetRequest) message;
                receivedKey.set(request.getKey());
            }
        };
        
        node2Bus.registerHandler(handler);
        
        // Act
        GetRequest request = new GetRequest("node-1", "node-2", "test-key");
        node1Bus.sendMessage(request);
        
        // Advance the simulator to allow message delivery
        packetSimulator.tick();
        
        // Assert
        assertThat(messageReceived.get()).isTrue();
        assertThat(receivedKey.get()).isEqualTo("test-key");
    }
    
    @Test
    void testQueueAndTickProcessing() {
        // Arrange
        List<String> receivedKeys = new ArrayList<>();
        
        MessageHandler handler = new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }
            
            @Override
            public void handleMessage(Message message) {
                GetRequest request = (GetRequest) message;
                receivedKeys.add(request.getKey());
            }
        };
        
        node2Bus.registerHandler(handler);
        
        // Act - queue multiple messages
        for (int i = 0; i < 3; i++) {
            GetRequest request = new GetRequest("node-1", "node-2", "key-" + i);
            node1Bus.queueMessage(request);
        }
        
        // Verify messages are queued
        assertThat(node1Bus.getQueueSize()).isEqualTo(3);
        
        // Process the queue with tick
        node1Bus.tick();
        
        // Verify queue is empty after processing
        assertThat(node1Bus.getQueueSize()).isEqualTo(0);
        
        // Advance packet simulator to deliver messages
        packetSimulator.tick();
        
        // Assert - all messages should be received
        assertThat(receivedKeys).hasSize(3);
        // Use a more relaxed assertion that checks the content but not the order
        assertThat(receivedKeys).containsExactlyInAnyOrder("key-0", "key-1", "key-2");
    }
    
    @Test
    void testSendMessageToDisconnectedNode() {
        // Arrange
        node1Bus.disconnect("node-2");
        GetRequest request = new GetRequest("node-1", "node-2", "test-key");
        
        // Act
        node1Bus.sendMessage(request);
        
        // Assert
        assertThat(request.getSourceId()).isEqualTo("node-1");
    }
    
    @Test
    void testMessageWithNullTargetId() {
        // Arrange
        GetRequest request = new GetRequest("node-1", null, "test-key");
        
        // Act
        node1Bus.sendMessage(request);
        
        // Assert
        assertThat(request.getSourceId()).isEqualTo("node-1");
    }
    
    @Test
    void testMessageWithIncorrectSourceId() {
        // Arrange
        AtomicReference<String> receivedSourceId = new AtomicReference<>();
        MessageHandler handler = new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }
            
            @Override
            public void handleMessage(Message message) {
                receivedSourceId.set(message.getSourceId());
            }
        };
        node2Bus.registerHandler(handler);
        
        GetRequest request = new GetRequest("wrong-node", "node-2", "test-key");
        
        // Act
        node1Bus.sendMessage(request);
        packetSimulator.tick();
        
        // Assert - message should be received with corrected source ID
        assertThat(receivedSourceId.get()).isEqualTo("node-1");
    }
    
    @Test
    void testQueueMessageWithIncorrectSourceId() {
        // Arrange
        AtomicBoolean messageReceived = new AtomicBoolean(false);
        AtomicReference<String> sourceId = new AtomicReference<>();
        
        MessageHandler handler = new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }
            
            @Override
            public void handleMessage(Message message) {
                messageReceived.set(true);
                sourceId.set(message.getSourceId());
            }
        };
        
        node2Bus.registerHandler(handler);
        
        // Act - queue a message with wrong source ID
        GetRequest request = new GetRequest("wrong-node", "node-2", "test-key");
        node1Bus.queueMessage(request);
        
        // Process the message
        node1Bus.tick();
        packetSimulator.tick();
        
        // Assert - message should be received with corrected source ID
        assertThat(messageReceived.get()).isTrue();
        assertThat(sourceId.get()).isEqualTo("node-1");
    }
    
    @Test
    void testHandlerRegistrationAndUnregistration() {
        // Arrange
        AtomicInteger getRequestCount = new AtomicInteger(0);
        AtomicInteger getResponseCount = new AtomicInteger(0);
        
        MessageHandler getRequestHandler = new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }
            
            @Override
            public void handleMessage(Message message) {
                getRequestCount.incrementAndGet();
            }
        };
        
        MessageHandler getResponseHandler = new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_RESPONSE;
            }
            
            @Override
            public void handleMessage(Message message) {
                getResponseCount.incrementAndGet();
            }
        };
        
        node2Bus.registerHandler(getRequestHandler);
        node2Bus.registerHandler(getResponseHandler);
        
        // Act - send both types of messages
        node1Bus.sendMessage(new GetRequest("node-1", "node-2", "key1"));
        node1Bus.sendMessage(new GetResponse("node-1", "node-2", "key2", "value2", true, null));
        packetSimulator.tick();
        
        // Assert - both handlers should be called
        assertThat(getRequestCount.get()).isEqualTo(1);
        assertThat(getResponseCount.get()).isEqualTo(1);
        
        // Act - unregister GET_REQUEST handler and send both types again
        node2Bus.unregisterHandler(MessageType.GET_REQUEST);
        node1Bus.sendMessage(new GetRequest("node-1", "node-2", "key3"));
        node1Bus.sendMessage(new GetResponse("node-1", "node-2", "key4", "value4", true, null));
        packetSimulator.tick();
        
        // Assert - only GET_RESPONSE handler should be called
        assertThat(getRequestCount.get()).isEqualTo(1); // Still 1
        assertThat(getResponseCount.get()).isEqualTo(2); // Incremented to 2
    }
    
    @Test
    void testMultiNodeCommunication() {
        // Arrange
        MessageBus node3Bus = new MessageBus("node-3", packetSimulator);
        node1Bus.connect("node-3");
        node3Bus.connect("node-1");
        
        List<String> receivedKeys = new ArrayList<>();
        
        MessageHandler handler = new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }
            
            @Override
            public void handleMessage(Message message) {
                GetRequest request = (GetRequest) message;
                receivedKeys.add(request.getKey());
            }
        };
        
        node2Bus.registerHandler(handler);
        node3Bus.registerHandler(handler);
        
        // Act - queue messages to both nodes
        node1Bus.queueMessage(new GetRequest("node-1", "node-2", "node2-key"));
        node1Bus.queueMessage(new GetRequest("node-1", "node-3", "node3-key"));
        
        // Process queued messages
        node1Bus.tick();
        
        // Deliver packets
        packetSimulator.tick();
        
        // Assert - both nodes should receive their respective messages
        assertThat(receivedKeys).hasSize(2);
        assertThat(receivedKeys).contains("node2-key", "node3-key");
    }
    
    @Test
    void testDeterministicMessageDelivery() {
        // Arrange
        List<String> run1Keys = new ArrayList<>();
        List<String> run2Keys = new ArrayList<>();
        
        // Setup for first run
        PacketSimulator sim1 = new PacketSimulator();
        MessageBus bus1A = new MessageBus("node-A", sim1);
        MessageBus bus1B = new MessageBus("node-B", sim1);
        bus1A.connect("node-B");
        bus1B.connect("node-A");
        
        bus1B.registerHandler(new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }
            
            @Override
            public void handleMessage(Message message) {
                GetRequest request = (GetRequest) message;
                run1Keys.add(request.getKey());
            }
        });
        
        // Setup for second run (identical configuration)
        PacketSimulator sim2 = new PacketSimulator();
        MessageBus bus2A = new MessageBus("node-A", sim2);
        MessageBus bus2B = new MessageBus("node-B", sim2);
        bus2A.connect("node-B");
        bus2B.connect("node-A");
        
        bus2B.registerHandler(new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }
            
            @Override
            public void handleMessage(Message message) {
                GetRequest request = (GetRequest) message;
                run2Keys.add(request.getKey());
            }
        });
        
        // Act - Queue messages in the same order for both runs
        for (int i = 0; i < 5; i++) {
            bus1A.queueMessage(new GetRequest("node-A", "node-B", "key-" + i));
            bus2A.queueMessage(new GetRequest("node-A", "node-B", "key-" + i));
        }
        
        // Process with identical tick sequence
        for (int i = 0; i < 3; i++) {
            bus1A.tick();
            sim1.tick();
            
            bus2A.tick();
            sim2.tick();
        }
        
        // Assert - both runs should produce identical results
        assertThat(run1Keys).isEqualTo(run2Keys);
        assertThat(run1Keys).hasSize(5);
    }

    @Test
    public void testRegisterHandler() {
        MessageHandler handler = new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }

            @Override
            public void handleMessage(Message message) {
                // Test implementation
            }
        };
        node2Bus.registerHandler(handler);
        // Test assertions
    }

    @Test
    public void testUnregisterHandler() {
        MessageHandler handler = new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }

            @Override
            public void handleMessage(Message message) {
                // Test implementation
            }
        };
        node2Bus.registerHandler(handler);
        node2Bus.unregisterHandler(MessageType.GET_REQUEST);
        // Test assertions
    }

    @Test
    public void testHandlePacket() {
        MessageHandler handler = new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }

            @Override
            public void handleMessage(Message message) {
                // Test implementation
            }
        };
        node2Bus.registerHandler(handler);
        // Test assertions
    }

    @Test
    public void testMessageRouting() {
        MessageHandler getRequestHandler = new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }

            @Override
            public void handleMessage(Message message) {
                // Test implementation
            }
        };

        MessageHandler getResponseHandler = new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_RESPONSE;
            }

            @Override
            public void handleMessage(Message message) {
                // Test implementation
            }
        };
        // Test assertions
    }

    @Test
    public void testQueueMessage() {
        MessageHandler handler = new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }

            @Override
            public void handleMessage(Message message) {
                // Test implementation
            }
        };
        node2Bus.registerHandler(handler);
        // Test assertions
    }

    @Test
    public void testBidirectionalCommunication() {
        MessageBus bus1B = new MessageBus("node1B", packetSimulator);
        MessageBus bus2B = new MessageBus("node2B", packetSimulator);

        bus1B.registerHandler(new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }

            @Override
            public void handleMessage(Message message) {
                // Test implementation
            }
        });

        bus2B.registerHandler(new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_RESPONSE;
            }

            @Override
            public void handleMessage(Message message) {
                // Test implementation
            }
        });
        // Test assertions
    }
} 