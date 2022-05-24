package org.easylibs.mq.dispatcher;

import java.util.HashMap;
import java.util.Map;

import org.easylibs.mq.IMQReadOnly;
import org.easylibs.mq.MQQueue;

public class MQBroadcastDispatcher implements IMQDispatcher {

    private Map<String, MQQueue> queues;

    public MQBroadcastDispatcher() {
        this.queues = new HashMap<>();
    }

    @Override
    public void declareQueue(String queueName) {
        synchronized (this.queues) {
            this.queues.putIfAbsent(queueName, new MQQueue());
        }
    }

    @Override
    public void reDeclareQueue(String queueName) {
        synchronized (this.queues) {
            declareQueue(queueName);
            this.queues.get(queueName).reopen();
        }
    }

    @Override
    public void queueBind(String pattern, String queueName) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public IMQReadOnly getQueue(String queueName) {
        synchronized (this.queues) {
            declareQueue(queueName);
            return this.queues.get(queueName);
        }
    }

    @Override
    public boolean put(String queueNameOrId, Object obj) {
        synchronized (this.queues) {
            return !this.queues.isEmpty() && this.queues.values().stream().map(q -> q.putItem(obj)).allMatch(Boolean.TRUE::equals);
        }
    }

    @Override
    public void release(String target) {
        synchronized (this.queues) {
            this.queues.forEach((k, v) -> v.close());
        }
    }

}
