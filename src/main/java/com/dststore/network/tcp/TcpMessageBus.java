package com.dststore.network.tcp;

import com.dststore.message.Message;
import com.dststore.message.MessageType;
import com.dststore.network.IMessageBus;
import com.dststore.network.MessageBus;
import com.dststore.network.MessageHandler;
import com.dststore.network.PacketListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TcpMessageBus implements IMessageBus {
    private static final Logger logger = LoggerFactory.getLogger(TcpMessageBus.class);
    private final String nodeId;
    private final int port;
    private final ObjectMapper objectMapper;
    private final Map<String, NodeConnection> connections;
    private final EventLoop eventLoop;
    private final Queue<Message> outgoingMessages;
    private final Queue<Message> completedReads;
    private ServerSocketChannel serverSocket;
    private volatile boolean running;
    private PacketListener packetListener;
    private final MessageBus messageBus;
    private final Map<SocketChannel, ReadState> readStates;
    private static final int MAX_RETRIES = 3;
    private static final long CONNECTION_TIMEOUT = 5000; // 5 seconds

    private static class ReadState {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        ByteBuffer messageBuffer;
        boolean readingLength = true;
    }

    private static class NodeConnection {
        private final String nodeId;
        private final SocketChannel socketChannel;
        private volatile boolean connected;
        private volatile long lastAttempt;
        private int retryCount;

        public NodeConnection(String nodeId, SocketChannel socketChannel) {
            this.nodeId = nodeId;
            this.socketChannel = socketChannel;
            this.connected = false;
            this.lastAttempt = System.currentTimeMillis();
            this.retryCount = 0;
        }

        public void markConnected() {
            this.connected = true;
            this.retryCount = 0;
        }

        public void markDisconnected() {
            this.connected = false;
        }

        public boolean isConnected() {
            return connected;
        }

        public boolean canRetry() {
            return retryCount < MAX_RETRIES && 
                   System.currentTimeMillis() - lastAttempt > CONNECTION_TIMEOUT;
        }

        public void incrementRetry() {
            retryCount++;
            lastAttempt = System.currentTimeMillis();
        }

        public void send(ByteBuffer buffer) throws IOException {
            if (!connected) {
                throw new IOException("Connection is not established");
            }
            while (buffer.hasRemaining()) {
                socketChannel.write(buffer);
            }
        }

        public void close() {
            try {
                connected = false;
                socketChannel.close();
            } catch (IOException e) {
                logger.error("Error closing connection to node {}", nodeId, e);
            }
        }
    }

    public TcpMessageBus(String nodeId, int port) throws IOException {
        this.nodeId = nodeId;
        this.port = port;
        this.objectMapper = new ObjectMapper();
        this.connections = new ConcurrentHashMap<>();
        this.eventLoop = new EventLoop();
        this.outgoingMessages = new ConcurrentLinkedQueue<>();
        this.completedReads = new ConcurrentLinkedQueue<>();
        this.readStates = new ConcurrentHashMap<>();
        this.running = false;
        this.messageBus = new MessageBus(nodeId, null);
    }

    /**
     * Advances the message bus by one time unit, processing any completed I/O operations.
     * This method ensures deterministic message delivery in the simulation.
     */
    public void tick() {
        if (!running) {
            return;
        }

        // Process any completed reads
        Message completedRead;
        while ((completedRead = completedReads.poll()) != null) {
            if (packetListener != null) {
                packetListener.onPacketReceived(completedRead);
            }
        }

        // Process any outgoing messages
        Message outgoingMessage;
        while ((outgoingMessage = outgoingMessages.poll()) != null) {
            doSendMessage(outgoingMessage);
        }

        // Drive the event loop
        eventLoop.tick();

        // Clean up any failed connections
        connections.values().removeIf(connection -> 
            !connection.isConnected() && !connection.canRetry());
    }

    @Override
    public void start() {
        try {
            serverSocket = ServerSocketChannel.open();
            serverSocket.bind(new InetSocketAddress(port));
            serverSocket.configureBlocking(false);
            running = true;

            // Register server socket for accept events
            eventLoop.register(serverSocket, SelectionKey.OP_ACCEPT, new AcceptHandler());
            eventLoop.start();

            logger.info("Started TCP MessageBus for node {} on port {}", nodeId, port);
        } catch (IOException e) {
            logger.error("Failed to start TCP MessageBus", e);
            throw new RuntimeException("Failed to start TCP MessageBus", e);
        }
    }

    private class AcceptHandler implements IOHandler {
        @Override
        public void handle(SelectionKey key) throws IOException {
            ServerSocketChannel server = (ServerSocketChannel) key.channel();
            SocketChannel clientSocket = server.accept();
            if (clientSocket != null) {
                clientSocket.configureBlocking(false);
                // Register for initial handshake read
                eventLoop.register(clientSocket, SelectionKey.OP_READ, new HandshakeHandler());
            }
        }
    }

    private class HandshakeHandler implements IOHandler {
        @Override
        public void handle(SelectionKey key) throws IOException {
            SocketChannel channel = (SocketChannel) key.channel();
            try {
                ByteBuffer buffer = ByteBuffer.allocate(1024);
                int bytesRead = channel.read(buffer);
                if (bytesRead > 0) {
                    buffer.flip();
                    byte[] data = new byte[buffer.remaining()];
                    buffer.get(data);
                    String remoteNodeId = new String(data).trim();

                    // Create and store the connection
                    NodeConnection connection = new NodeConnection(remoteNodeId, channel);
                    connection.markConnected();
                    connections.put(remoteNodeId, connection);

                    // Register for message reading
                    eventLoop.register(channel, SelectionKey.OP_READ, new ReadHandler(connection));

                    logger.info("Established connection with node {}", remoteNodeId);
                }
            } catch (IOException e) {
                logger.error("Error handling handshake", e);
                try {
                    channel.close();
                } catch (IOException ex) {
                    logger.error("Error closing channel", ex);
                }
            }
        }
    }

    private class ReadHandler implements IOHandler {
        private final NodeConnection connection;

        public ReadHandler(NodeConnection connection) {
            this.connection = connection;
        }

        @Override
        public void handle(SelectionKey key) throws IOException {
            SocketChannel channel = (SocketChannel) key.channel();
            try {
                ReadState state = readStates.computeIfAbsent(channel, k -> new ReadState());

                if (state.readingLength) {
                    // Read message length
                    int bytesRead = channel.read(state.lengthBuffer);
                    if (bytesRead == -1) {
                        // Connection closed
                        disconnect(connection.nodeId);
                        return;
                    }
                    if (state.lengthBuffer.hasRemaining()) {
                        return; // Need more bytes for length
                    }

                    // Got complete length, prepare for message
                    state.lengthBuffer.flip();
                    int messageLength = state.lengthBuffer.getInt();
                    state.messageBuffer = ByteBuffer.allocate(messageLength);
                    state.readingLength = false;
                    state.lengthBuffer.clear();
                }

                // Read message content
                int bytesRead = channel.read(state.messageBuffer);
                if (bytesRead == -1) {
                    // Connection closed
                    disconnect(connection.nodeId);
                    return;
                }
                if (state.messageBuffer.hasRemaining()) {
                    return; // Need more bytes for message
                }

                // Process complete message
                state.messageBuffer.flip();
                byte[] messageData = new byte[state.messageBuffer.remaining()];
                state.messageBuffer.get(messageData);
                Message message = objectMapper.readValue(messageData, Message.class);

                // Queue the message for processing on next tick
                completedReads.offer(message);

                // Reset state for next message
                state.readingLength = true;
                state.messageBuffer = null;
            } catch (IOException e) {
                if (running) {
                    logger.error("Error reading from connection to {}", connection.nodeId, e);
                }
                disconnect(connection.nodeId);
            }
        }
    }

    @Override
    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
            for (NodeConnection connection : connections.values()) {
                connection.close();
            }
            eventLoop.stop();
        } catch (IOException e) {
            logger.error("Error stopping TCP MessageBus", e);
        }
    }

    @Override
    public void connect(String targetNodeId, String host, int port) {
        eventLoop.execute(() -> {
            try {
                SocketChannel socketChannel = SocketChannel.open();
                socketChannel.configureBlocking(false);
                socketChannel.connect(new InetSocketAddress(host, port));

                // Create and store the connection
                NodeConnection connection = new NodeConnection(targetNodeId, socketChannel);
                connections.put(targetNodeId, connection);

                // Wait for connection to complete
                while (!socketChannel.finishConnect()) {
                    Thread.sleep(10);
                }

                // Send our node ID
                ByteBuffer buffer = ByteBuffer.wrap(nodeId.getBytes());
                while (buffer.hasRemaining()) {
                    socketChannel.write(buffer);
                }

                // Mark as connected and register for reading
                connection.markConnected();
                eventLoop.register(socketChannel, SelectionKey.OP_READ, new ReadHandler(connection));

                logger.info("Connected to node {} at {}:{}", targetNodeId, host, port);
            } catch (IOException | InterruptedException e) {
                logger.error("Failed to connect to node {} at {}:{}", targetNodeId, host, port, e);
                NodeConnection failedConnection = connections.remove(targetNodeId);
                if (failedConnection != null) {
                    failedConnection.close();
                }
                throw new RuntimeException("Failed to connect to node", e);
            }
        });
    }

    @Override
    public void disconnect(String targetNodeId) {
        NodeConnection connection = connections.remove(targetNodeId);
        if (connection != null) {
            connection.close();
            readStates.remove(connection.socketChannel);
            logger.info("Disconnected from node {}", targetNodeId);
        }
    }

    @Override
    public void sendMessage(Message message) {
        if (message.getTargetId() == null) {
            logger.error("Cannot send message with null target ID: {}", message);
            return;
        }

        if (!nodeId.equals(message.getSourceId())) {
            logger.warn("Message source ID is not this node ({} vs {}), correcting", 
                message.getSourceId(), nodeId);
            message = message.withSourceId(nodeId);
        }

        // Queue the message for sending on next tick
        outgoingMessages.offer(message);
    }

    private void doSendMessage(Message message) {
        String targetId = message.getTargetId();
        if (targetId == null) {
            logger.error("Cannot send message with null target ID: {}", message);
            return;
        }

        NodeConnection connection = connections.get(targetId);
        if (connection == null) {
            logger.warn("No connection found for target node {}", targetId);
            return;
        }

        if (!connection.isConnected()) {
            if (connection.canRetry()) {
                connection.incrementRetry();
                outgoingMessages.offer(message);
                logger.warn("Connection to {} not ready, requeueing message", targetId);
            } else {
                logger.error("Failed to send message after {} retries", MAX_RETRIES);
                disconnect(targetId);
            }
            return;
        }

        try {
            byte[] messageData = objectMapper.writeValueAsBytes(message);
            ByteBuffer buffer = ByteBuffer.allocate(4 + messageData.length);
            buffer.putInt(messageData.length);
            buffer.put(messageData);
            buffer.flip();

            connection.send(buffer);
        } catch (IOException e) {
            logger.error("Error sending message to {}", targetId, e);
            connection.markDisconnected();
            if (connection.canRetry()) {
                connection.incrementRetry();
                outgoingMessages.offer(message);
            } else {
                disconnect(targetId);
            }
        }
    }

    @Override
    public void registerListener(PacketListener listener) {
        this.packetListener = listener;
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public void registerHandler(MessageHandler messageHandler) {
        // Forward to the underlying message bus
        messageBus.registerHandler(messageHandler);
    }
    
    @Override
    public void unregisterHandler(MessageType messageType) {
        // Forward to the underlying message bus
        messageBus.unregisterHandler(messageType);
    }

    /**
     * Returns the port number this message bus is bound to.
     */
    public int getPort() {
        return port;
    }
} 