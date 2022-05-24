package org.easylibs.mq;

import java.util.HashMap;
import java.util.Map;

import org.easylibs.mq.dispatcher.IMQDispatcher;
import org.easylibs.mq.dispatcher.MQBroadcastDispatcher;
import org.easylibs.mq.dispatcher.MQPatternedDispatcher;
import org.easylibs.mq.dispatcher.MQSharedDispatcher;
import org.easylibs.mq.exception.UndeclaredException;

public class MQManager {

    public static enum MQType {
        BROADCAST, TOPIC, SHARED
    }

    public static IMQDispatcher getMQDispatcher(MQType type) {
        switch (type) {
        case BROADCAST:
            return new MQBroadcastDispatcher();
        case TOPIC: 
            return new MQPatternedDispatcher();
        case SHARED:
            return new MQSharedDispatcher();
        default: 
            return null;
        }
    }

    private final String DEFAULT_DISPATCHER;
    private Map<String, IMQDispatcher> dispatchers;

    public MQManager() {
        this.DEFAULT_DISPATCHER = this.toString();
        this.dispatchers = new HashMap<>();
        this.dispatchers.put(DEFAULT_DISPATCHER, MQManager.getMQDispatcher(MQType.SHARED));
    }

    /**
     * <p>{@code MQType.BROADCAST} dispatches data to all queues it manages
     * <p>{@code MQType.TOPIC} dispatches data by patterns to binded queues
     * <p>{@code MQType.SHARED} dispatches data to a specific queue
     * 
     * @param dispatcherName - {@link String}
     * @param type - {@link MQType}
     */
    public void declareDispatcher(String dispatcherName, MQType type) {
        synchronized (this.dispatchers) {
            this.dispatchers.putIfAbsent(dispatcherName, MQManager.getMQDispatcher(type));
        }
    }

    public IMQDispatcher getDispatcher(String dispatcherName) throws UndeclaredException {
        synchronized (this.dispatchers) {
            if (!this.dispatchers.containsKey(dispatcherName)) {
                throw new UndeclaredException();
            }
            return this.dispatchers.get(dispatcherName);
        }
    }

    public void declareQueue(String queueName) {
        synchronized (this.dispatchers) {
            this.dispatchers.get(DEFAULT_DISPATCHER).declareQueue(queueName);
        }
    }

    public IMQReadOnly getQueue(String queueName) {
        synchronized (this.dispatchers) {
            return this.dispatchers.get(DEFAULT_DISPATCHER).getQueue(queueName);
        }
    }

    public boolean put(String queueName, Object obj) {
        synchronized (this.dispatchers) {
            return this.dispatchers.get(DEFAULT_DISPATCHER).put(queueName, obj);
        }
    }

    public void releaseDispatcher(String dispatcherName) {
        synchronized (this.dispatchers) {
            this.dispatchers.get(dispatcherName).release(null);
            this.dispatchers.remove(dispatcherName);
        }
    }

    public void releaseQueue(String queueName) {
        synchronized (this.dispatchers) {
            this.dispatchers.get(DEFAULT_DISPATCHER).release(queueName);
        }
    }
    
}
