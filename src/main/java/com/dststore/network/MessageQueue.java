package com.dststore.network;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages a priority queue of scheduled messages for the simulated network.
 * This class handles the scheduling, queuing, and retrieval of messages based on delivery ticks.
 */
public class MessageQueue {
    private static final Logger LOGGER = Logger.getLogger(MessageQueue.class.getName());

    // Message sequence counter for FIFO ordering when messages have the same delivery tick
    private final AtomicLong messageSequence = new AtomicLong(0);

    // Priority queue for message scheduling
    private final PriorityBlockingQueue<ScheduledMessage> messageQueue;


    /**
     * Class representing a scheduled message.
     */
    public static class ScheduledMessage {
        final Object message;
        final String from;
        final String to;
        final long deliveryTick;
        final long sequenceNumber;

        public ScheduledMessage(Object message, String from, String to, long deliveryTick,
                         long sequenceNumber) {
            this.message = message;
            this.from = from;
            this.to = to;
            this.deliveryTick = deliveryTick;
            this.sequenceNumber = sequenceNumber;
        }

        public Object getMessage() {
            return message;
        }

        public String getFrom() {
            return from;
        }

        public String getTo() {
            return to;
        }

        public long getDeliveryTick() {
            return deliveryTick;
        }

        public long getSequenceNumber() {
            return sequenceNumber;
        }
    }

    public MessageQueue() {
        this.messageQueue = new PriorityBlockingQueue<>(
                100, Comparator.<ScheduledMessage>comparingLong(message -> message.deliveryTick)
                .thenComparingLong(message -> message.sequenceNumber));
    }

    public void clear() {
        messageQueue.clear();
    }

    public int size() {
        return messageQueue.size();
    }

    public boolean isEmpty() {
        return messageQueue.isEmpty();
    }

    public ScheduledMessage peek() {
        return messageQueue.peek();
    }

    public ScheduledMessage poll() {
        return messageQueue.poll();
    }

    /**
     * Schedules a message for delivery at the specified tick.
     */
    public ScheduledMessage scheduleMessage(Object message, String from, String to, long deliveryTick) {
        ScheduledMessage scheduledMessage = new ScheduledMessage(
                message, from, to, deliveryTick, messageSequence.getAndIncrement());
        messageQueue.add(scheduledMessage);

        String messageType = message.getClass().getSimpleName();
        LOGGER.log(Level.INFO, "Message type {0} scheduled for delivery at tick {1}: {2} -> {3}",
                new Object[]{messageType, deliveryTick, from, to});

        return scheduledMessage;
    }

    /**
     * Gets all messages that are due for delivery at or before the specified tick.
     * It also applies the bandwidth limit if the messages for this tick are more than
     * maxMessagesPerTick
     */
    public List<ScheduledMessage> getMessagesForTick(long currentTick, int maxMessagesPerTick) {
        List<ScheduledMessage> messagesForThisTick = new ArrayList<>();
        
        while (!messageQueue.isEmpty() && messageQueue.peek().deliveryTick <= currentTick) {
            ScheduledMessage msg = messageQueue.peek();
            LOGGER.log(Level.INFO, "Found message due for delivery: from {0} to {1}, type {2}, scheduled for tick {3}",
                    new Object[]{msg.from, msg.to, msg.message.getClass().getSimpleName(), msg.deliveryTick});
            messagesForThisTick.add(messageQueue.poll());
        }
        
        return applyBandwidthLimit(messagesForThisTick, maxMessagesPerTick,currentTick);
    }


    private List<MessageQueue.ScheduledMessage> applyBandwidthLimit(List<MessageQueue.ScheduledMessage> messagesForThisTick, int maxMessagesPerTick, long currentTick) {
        // Apply bandwidth limit if necessary
        if (messagesForThisTick.size() > maxMessagesPerTick) {
            List<MessageQueue.ScheduledMessage> deferredMessages =
                    messagesForThisTick.subList(maxMessagesPerTick, messagesForThisTick.size());

            // Reschedule excess messages to the next tick
            for (MessageQueue.ScheduledMessage message : deferredMessages) {
                rescheduleMessage(
                        message.getMessage(),
                        message.getFrom(),
                        message.getTo(),
                        currentTick + 1
                );
            }

            // Keep only the messages within the bandwidth limit
            messagesForThisTick = messagesForThisTick.subList(0, maxMessagesPerTick);

            LOGGER.log(Level.FINE, "Bandwidth limit applied: {0} messages deferred to next tick",
                    deferredMessages.size());
        }
        return messagesForThisTick;
    }

    /**
     * Reschedules a message for delivery at a new tick.
     *
     * @param message The original message
     * @param from The sender node ID
     * @param to The recipient node ID
     * @param newDeliveryTick The new delivery tick
     * @return The rescheduled message
     */
    public ScheduledMessage rescheduleMessage(Object message, String from, String to, long newDeliveryTick) {
        return scheduleMessage(message, from, to, newDeliveryTick);
    }
} 