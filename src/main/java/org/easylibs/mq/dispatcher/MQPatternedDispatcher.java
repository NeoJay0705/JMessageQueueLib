package org.easylibs.mq.dispatcher;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.easylibs.mq.IMQReadOnly;
import org.easylibs.mq.MQQueue;

public class MQPatternedDispatcher implements IMQDispatcher {

    private Map<String, MQQueue> queues;
    private Map<String, Set<String>> router;

    public MQPatternedDispatcher() {
        this.queues = new HashMap<>();
        this.router = new HashMap<>();
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
        synchronized (this.router) {
            this.router.putIfAbsent(pattern, new HashSet<>());
            this.router.get(pattern).add(queueName);
        }
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
        Set<MQQueue> inserted = new HashSet<>();
        boolean status = true;
        boolean allMiss = true;
        synchronized (this.router) {

            // allMiss = !this.router.keySet().stream().anyMatch(k -> this.matched(queueNameOrId, k));
            // synchronized (this.queues) {
            //     status = this.router.entrySet().stream()
            //     .filter(e -> this.matched(queueNameOrId, e.getKey()))
            //     .map(Entry::getValue)
            //     .map(Set::stream).map(
            //         s -> s.map(this.queues::get).filter(q -> !inserted.contains(q))
            //               .peek(inserted::add).map(q -> q.putItem(obj))
            //               .allMatch(Boolean.TRUE::equals)
            //     ).allMatch(Boolean.TRUE::equals);
            // }
            
            // allMiss = !this.router.keySet().stream().anyMatch(k -> this.matched(queueNameOrId, k));
            // synchronized (this.queues) {
            //     this.router.entrySet().stream()
            //         .filter(e -> this.matched(queueNameOrId, e.getKey()))
            //         .map(Entry::getValue) .map(Set::stream)
            //         .forEach(s -> s.map(this.queues::get).forEach(inserted::add));
            //     status = inserted.stream().map(q -> q.putItem(obj)).allMatch(Boolean.TRUE::equals);
            // }

            // allMiss = !this.router.keySet().stream().anyMatch(k -> this.matched(queueNameOrId, k));
            // synchronized (this.queues) {
            //     for (Entry<String, Set<String>> e : this.router.entrySet()) { // this.router.entrySet().stream()
            //         if (this.matched(queueNameOrId, e.getKey())) { // filter(e -> this.matched(queueNameOrId, e.getKey()))
            //             for (String s : e.getValue()) { // s.map(this.queues::get)
            //                 inserted.add(this.queues.get(s)); // forEach(inserted::add))
            //             }
            //         }
            //     }
            //     for (MQQueue q : inserted) { // inserted.stream()
            //         status &= /* allMatch(Boolean.TRUE::equals) */ q.putItem(obj); // map(q -> q.putItem(obj))
            //     }
            // }

            for (Entry<String, Set<String>> patternToQueueName : this.router.entrySet()) {
                if (this.matched(queueNameOrId, patternToQueueName.getKey())) {
                    allMiss = false;
                    for (String queueName : patternToQueueName.getValue()) {
                        synchronized (this.queues) {
                            MQQueue targetQueue = this.queues.get(queueName);
                            if (!inserted.contains(targetQueue)) {
                                status &= targetQueue.putItem(obj);
                                inserted.add(targetQueue);
                            }
                        }
                    }
                }
            }
        }
        return !allMiss && status;
    }
    
    private boolean matched(String input, String pattern) {
        int i = 0;
        int j = 0;
        while (i < pattern.length() && j < input.length()) {
            if (pattern.charAt(i) == '#')
                return true;
            if (pattern.charAt(i) == '*') {
                if (input.charAt(j) == '.') {
                    i += 2;
                }
                j++;
            } else {
                if (pattern.charAt(i) != input.charAt(j)) {
                    return false;
                }
                i++;
                j++;
            }
        }
        return (pattern.length() == i || pattern.charAt(i) == '*') && input.length() == j;
    }

    @Override
    public void release(String target) {
        synchronized (this.queues) {
            this.queues.forEach((k, v) -> v.close());
        }
    }

}
