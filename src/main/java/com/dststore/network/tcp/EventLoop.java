package com.dststore.network.tcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Event loop for handling asynchronous I/O operations.
 */
public class EventLoop implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(EventLoop.class);
    
    private final Selector selector;
    private final Queue<Runnable> tasks;
    private volatile boolean running;
    private static final long SELECT_TIMEOUT = 100; // 100ms timeout for select
    private Thread eventLoopThread;
    
    public EventLoop() throws IOException {
        this.selector = Selector.open();
        this.tasks = new ConcurrentLinkedQueue<>();
        this.running = false;
    }
    
    public void start() {
        if (!running) {
            running = true;
            eventLoopThread = new Thread(this);
            eventLoopThread.start();
        }
    }
    
    public void stop() {
        running = false;
        if (eventLoopThread != null) {
            eventLoopThread.interrupt();
            try {
                eventLoopThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            selector.close();
        } catch (IOException e) {
            logger.error("Error closing selector", e);
        }
    }
    
    @Override
    public void run() {
        while (running) {
            try {
                // Process any pending tasks
                Runnable task;
                while ((task = tasks.poll()) != null) {
                    task.run();
                }

                // Wait for I/O events with timeout
                int readyOps = selector.select(SELECT_TIMEOUT);
                if (readyOps > 0) {
                    Set<SelectionKey> selectedKeys = selector.selectedKeys();
                    Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                    while (keyIterator.hasNext()) {
                        SelectionKey key = keyIterator.next();
                        keyIterator.remove();

                        if (!key.isValid()) {
                            continue;
                        }

                        try {
                            IOHandler handler = (IOHandler) key.attachment();
                            handler.handle(key);
                        } catch (Exception e) {
                            logger.error("Error handling I/O operation", e);
                            key.cancel();
                            try {
                                key.channel().close();
                            } catch (IOException ex) {
                                logger.error("Error closing channel", ex);
                            }
                        }
                    }
                }
            } catch (IOException e) {
                if (running) {
                    logger.error("Event loop error", e);
                }
            }
        }
    }
    
    public void execute(Runnable task) {
        tasks.offer(task);
        selector.wakeup();
    }
    
    public void register(SelectableChannel channel, int ops, IOHandler handler) throws IOException {
        if (Thread.currentThread() == eventLoopThread) {
            channel.register(selector, ops, handler);
        } else {
            execute(() -> {
                try {
                    channel.register(selector, ops, handler);
                } catch (IOException e) {
                    logger.error("Error registering channel", e);
                }
            });
        }
    }
    
    public void tick() {
        // Process any pending tasks
        Runnable task;
        while ((task = tasks.poll()) != null) {
            try {
                task.run();
            } catch (Exception e) {
                logger.error("Error executing task", e);
            }
        }

        try {
            // Process I/O events
            if (selector.selectNow() > 0) {
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    try {
                        IOHandler handler = (IOHandler) key.attachment();
                        handler.handle(key);
                    } catch (Exception e) {
                        logger.error("Error handling I/O operation", e);
                        key.cancel();
                        try {
                            key.channel().close();
                        } catch (IOException ex) {
                            logger.error("Error closing channel", ex);
                        }
                    }
                }
            }
        } catch (IOException e) {
            if (running) {
                logger.error("Event loop error", e);
            }
        }
    }
} 