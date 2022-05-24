package org.easylibs.mq;

/**
 * <p>IMQReadOnly is used to avoid unnecessary accessing for consumers.</p>
 */
public interface IMQReadOnly {
    /**
     * Same as {@link com.iisigroup.extension.messagequeue.MQQueue.getItem}
     * 
     * @return {@link Object}
     */
    public Object mqRead();
}
