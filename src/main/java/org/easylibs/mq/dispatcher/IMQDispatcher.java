package org.easylibs.mq.dispatcher;

import org.easylibs.mq.IMQReadOnly;

/**
 * <p>A dispatcher manages queues and dispatches data to specific queue(s).</p>
 * 
 * <p>Before, whatever a provider or a consumer, you should {@code declareQueue(queueName)}.</p>
 */
public interface IMQDispatcher {
    public void declareQueue(String queueName);

    public void reDeclareQueue(String queueName);

    /**
     * <p>A pattern has specific format for dispatching items to queues.</p>
     * 
     * <p>The patern is obligible in {@code PATTERN[.PATTERN]}</p>
     * <p>The {@code PATTERN} is {@code *|HOLE_WORD|#}</p>
     * <p>{@code *} is any string until dot</p>
     * <p>{@code #} is any string</p>
     * 
     * @param pattern - {@link String}
     * @param queueName - {@link String}
     */
    public void queueBind(String pattern, String queueName);

    /**
     * Returns a queue with limited accessing.
     * 
     * @param queueName - {@link String}
     * @return
     */
    public IMQReadOnly getQueue(String queueName);

    /**
     * <p>
     * Put the {@code obj} to the queue by {@code queueName}
     * , or {@code pattern} that will dispatch the {@code obj} to the specific queues by rules 
     * defined from {@code queueBind(pattern, queueName).}
     * </p>
     * 
     * @param queueNameOrId {@link String}
     * @param obj {@link Object}
     * @return {@code boolean}
     */
    public boolean put(String queueNameOrId, Object obj);

    /**
     * <p>Close queues by queueName it is managing.</p>
     * 
     * @param target - {@link String}
     */
    public void release(String target);

}
