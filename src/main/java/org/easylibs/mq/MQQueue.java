package org.easylibs.mq;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A queue is used for trensfering data between threads.
 */
public class MQQueue implements IMQReadOnly {
    private Deque<Object> queue;
    private boolean closed;

    public MQQueue() {
        this.queue = new ArrayDeque<>();
    }

    /**
     * <p>Consumers get items from the queue.</p>
     * 
     * <p>If the queue is not empty, the consumers get until it empty.</p>
     * 
     * <p>If the queue is empty, the consumers will check the {@code closed} flag to determine next step.</p>
     * <p>If the {@code closed} is true, this returns {@code null}.</p>
     * <pre>If the {@code closed} is {@code false}, thie consumers blocked until
     *     1. A provider releases the queue.
     *     2. A provider inserts an item to the queue.
     * </pre>
     * 
     * @return {@link Object}
     */
    public Object getItem() {
        while (true) {
            synchronized (this) {
                if (queue.isEmpty()) {
                    if (closed) 
                        return null;
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return null;
                    }
                } else {
                    return this.queue.pollFirst();
                }
            }
        }
    }

    /**
     * <p>Providers put items to the queue.</p>
     * 
     * <p>It returns {@code false} if the queue is closed. Otherwise, it returns {@code true}.</p>
     * 
     * @param obj - {@link Object}
     * @return {@link boolean}
     */
    public boolean putItem(Object obj) {
        synchronized (this) {
            if (closed) {
                return false;
            }
            this.queue.addLast(obj);
            this.notify();
        }
        return true;
    }

    /**
     * The {@code close()} is too safe to lose items in the queue.
     */
    public void close() {
        synchronized (this) {
            this.closed = true;
            this.notifyAll();
        }
    }

    public void reopen() {
        synchronized (this) {
            this.closed = false;
        }
    }

    @Override
    public Object mqRead() {
        return this.getItem();
    }
}
