package org.easylibs.mq;

import java.util.ArrayList;
import java.util.List;

import org.easylibs.mq.MQManager.MQType;
import org.easylibs.mq.dispatcher.IMQDispatcher;
import org.easylibs.mq.exception.UndeclaredException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMQ {

    @Test
    public void provideBeforeConsume() throws InterruptedException {
        List<String> reuslt = new ArrayList<>();

        MQManager mqm = new MQManager();

        Thread t1 = new Thread(() -> {
            mqm.declareQueue("a");
            if (!mqm.put("a", "123")) reuslt.add("put 123 fail");
            mqm.releaseQueue("a");
        });
        Thread t2 = new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            mqm.declareQueue("a");
            IMQReadOnly imqr = mqm.getQueue("a");
            if (!"123".equals(imqr.mqRead())) reuslt.add("consume 123 fail");
            if (imqr.mqRead() != null) reuslt.add("t2 not null");
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        Assertions.assertEquals(0, reuslt.size());
    }

    @Test
    public void consumeBeforeProvide() throws InterruptedException {
        List<String> reuslt = new ArrayList<>();

        MQManager mqm = new MQManager();

        Thread t1 = new Thread(() -> {
            mqm.declareQueue("a");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (!mqm.put("a", "123")) reuslt.add("err");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            mqm.releaseQueue("a");
        });
        Thread t2 = new Thread(() -> {
            mqm.declareQueue("a");
            IMQReadOnly imqr = mqm.getQueue("a");
            if (!"123".equals(imqr.mqRead())) reuslt.add("123");
            if (imqr.mqRead() != null) reuslt.add("t2 not null");
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        Assertions.assertEquals(0, reuslt.size());
    }

    @Test
    public void rrConsume() throws InterruptedException {
        List<String> reuslt = new ArrayList<>();

        MQManager mqm = new MQManager();

        Thread t1 = new Thread(() -> {
            mqm.declareQueue("a");
            if (!mqm.put("a", "123")) reuslt.add("err");
            if (!mqm.put("a", "456")) reuslt.add("err");
            if (!mqm.put("a", "789")) reuslt.add("err");
            mqm.releaseQueue("a");
        });
        Thread t2 = new Thread(() -> {
            mqm.declareQueue("a");
            IMQReadOnly imqr = mqm.getQueue("a");

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (!"123".equals(imqr.mqRead())) reuslt.add("123");
            try {
                Thread.sleep(600);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (!"789".equals(imqr.mqRead())) reuslt.add("789");
            if (imqr.mqRead() != null) reuslt.add("t2 not null");
        });
        Thread t3 = new Thread(() -> {
            mqm.declareQueue("a");
            IMQReadOnly imqr = mqm.getQueue("a");

            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (!"456".equals(imqr.mqRead())) reuslt.add("456");
            try {
                Thread.sleep(600);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (imqr.mqRead() != null) reuslt.add("t3 not null");
        });
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        Assertions.assertEquals(0, reuslt.size());
    }

    @Test
    public void branchBeforeBinding() {
        MQManager mqm = new MQManager();
        mqm.declareDispatcher("d1", MQType.BROADCAST);
        try {
            Assertions.assertEquals(false, mqm.getDispatcher("d1").put(null, "123"));
        } catch (UndeclaredException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void broadcast() throws InterruptedException {
        List<String> reuslt = new ArrayList<>();

        MQManager mqm = new MQManager();

        Thread t1 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.BROADCAST);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                if (!mqm.getDispatcher("d1").put(null, "123")) reuslt.add("err");
                if (!mqm.getDispatcher("d1").put(null, "456")) reuslt.add("err");
                if (!mqm.getDispatcher("d1").put(null, "789")) reuslt.add("err");
                mqm.getDispatcher("d1").release(null);
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        Thread t2 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.BROADCAST);
                
            try {
                IMQDispatcher imqd = mqm.getDispatcher("d1");
                imqd.declareQueue("d1.t2");
                IMQReadOnly imqr = imqd.getQueue("d1.t2");
                if (!"123".equals(imqr.mqRead())) reuslt.add("123");
                if (!"456".equals(imqr.mqRead())) reuslt.add("456");
                if (!"789".equals(imqr.mqRead())) reuslt.add("789");
                if (imqr.mqRead() != null) reuslt.add("t2 not null");
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        Thread t3 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.BROADCAST);
                
            try {
                IMQDispatcher imqd = mqm.getDispatcher("d1");
                imqd.declareQueue("d1.t3");
                IMQReadOnly imqr = imqd.getQueue("d1.t3");
                if (!"123".equals(imqr.mqRead())) reuslt.add("123");
                if (!"456".equals(imqr.mqRead())) reuslt.add("456");
                if (!"789".equals(imqr.mqRead())) reuslt.add("789");
                if (imqr.mqRead() != null) reuslt.add("t3 not null");
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        Assertions.assertEquals(0, reuslt.size());
    }

    @Test
    public void oneToOne_branch() throws InterruptedException {
        List<String> reuslt = new ArrayList<>();

        MQManager mqm = new MQManager();

        Thread t1 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                if (!mqm.getDispatcher("d1").put("title_a", "123")) reuslt.add("err");
                if (!mqm.getDispatcher("d1").put("title_b", "456")) reuslt.add("err");
                if (!mqm.getDispatcher("d1").put("title_a", "789")) reuslt.add("err");
                mqm.getDispatcher("d1").release(null);
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        Thread t2 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);
                
            try {
                IMQDispatcher imqd = mqm.getDispatcher("d1");
                imqd.declareQueue("d1.t2");
                imqd.queueBind("title_a", "d1.t2");
                IMQReadOnly imqr = imqd.getQueue("d1.t2");
                if (!"123".equals(imqr.mqRead())) reuslt.add("123");
                if (!"789".equals(imqr.mqRead())) reuslt.add("789");
                if (imqr.mqRead() != null) reuslt.add("t2 not null");
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        Thread t3 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);
                
            try {
                IMQDispatcher imqd = mqm.getDispatcher("d1");
                imqd.declareQueue("d1.t3");
                imqd.queueBind("title_b", "d1.t3");
                IMQReadOnly imqr = imqd.getQueue("d1.t3");
                if (!"456".equals(imqr.mqRead())) reuslt.add("456");
                if (imqr.mqRead() != null) reuslt.add("t3 not null");
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        Assertions.assertEquals(0, reuslt.size());
    }

    @Test
    public void manyToOne_branch() throws InterruptedException {
        List<String> reuslt = new ArrayList<>();

        MQManager mqm = new MQManager();

        Thread t1 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                if (!mqm.getDispatcher("d1").put("title_a", "123")) reuslt.add("err");
                if (!mqm.getDispatcher("d1").put("title_b", "456")) reuslt.add("err");
                if (!mqm.getDispatcher("d1").put("title_a", "789")) reuslt.add("err");
                mqm.getDispatcher("d1").release(null);
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        Thread t2 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);
                
            try {
                IMQDispatcher imqd = mqm.getDispatcher("d1");
                imqd.declareQueue("d1.t2");
                imqd.queueBind("title_a", "d1.t2");
                imqd.queueBind("title_b", "d1.t2");
                IMQReadOnly imqr = imqd.getQueue("d1.t2");
                if (!"123".equals(imqr.mqRead())) reuslt.add("123");
                if (!"456".equals(imqr.mqRead())) reuslt.add("456");
                if (!"789".equals(imqr.mqRead())) reuslt.add("789");
                if (imqr.mqRead() != null) reuslt.add("t2 not null");
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        Thread t3 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);
                
            try {
                IMQDispatcher imqd = mqm.getDispatcher("d1");
                imqd.declareQueue("d1.t3");
                imqd.queueBind("title_b", "d1.t3");
                IMQReadOnly imqr = imqd.getQueue("d1.t3");
                if (!"456".equals(imqr.mqRead())) reuslt.add("456");
                if (imqr.mqRead() != null) reuslt.add("t3 not null");
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        Assertions.assertEquals(0, reuslt.size());
    }

    @Test
    public void partAnyString_atFirstSlot_topic() throws InterruptedException {
        List<String> reuslt = new ArrayList<>();

        MQManager mqm = new MQManager();

        Thread t1 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                if (!mqm.getDispatcher("d1").put("1.a.12", "123")) reuslt.add("err");
                if (!mqm.getDispatcher("d1").put("2.a.12", "456")) reuslt.add("err");
                mqm.getDispatcher("d1").release(null);
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        Thread t2 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);
                
            try {
                IMQDispatcher imqd = mqm.getDispatcher("d1");
                imqd.declareQueue("d1.t2");
                imqd.queueBind("*.a.12", "d1.t2");
                IMQReadOnly imqr = imqd.getQueue("d1.t2");
                if (!"123".equals(imqr.mqRead())) reuslt.add("123");
                if (!"456".equals(imqr.mqRead())) reuslt.add("456");
                if (imqr.mqRead() != null) reuslt.add("t2 not null");
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        Thread t3 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);
                
            try {
                IMQDispatcher imqd = mqm.getDispatcher("d1");
                imqd.declareQueue("d1.t3");
                imqd.queueBind("*.a.12", "d1.t3");
                IMQReadOnly imqr = imqd.getQueue("d1.t3");
                if (!"123".equals(imqr.mqRead())) reuslt.add("123");
                if (!"456".equals(imqr.mqRead())) reuslt.add("456");
                if (imqr.mqRead() != null) reuslt.add("t3 not null");
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        Assertions.assertEquals(0, reuslt.size());
    }

    @Test
    public void partAnyString_atMiddleSlot_topic() throws InterruptedException {
        List<String> reuslt = new ArrayList<>();

        MQManager mqm = new MQManager();

        Thread t1 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                if (!mqm.getDispatcher("d1").put("1.a.12", "123")) reuslt.add("err");
                if (!mqm.getDispatcher("d1").put("2.b.12", "456")) reuslt.add("err");
                if (!mqm.getDispatcher("d1").put("1.b.12", "789")) reuslt.add("err");
                mqm.getDispatcher("d1").release(null);
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        Thread t2 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);
                
            try {
                IMQDispatcher imqd = mqm.getDispatcher("d1");
                imqd.declareQueue("d1.t2");
                imqd.queueBind("1.*.12", "d1.t2");
                IMQReadOnly imqr = imqd.getQueue("d1.t2");
                if (!"123".equals(imqr.mqRead())) reuslt.add("123");
                if (!"789".equals(imqr.mqRead())) reuslt.add("789");
                if (imqr.mqRead() != null) reuslt.add("t2 not null");
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        Thread t3 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);
                
            try {
                IMQDispatcher imqd = mqm.getDispatcher("d1");
                imqd.declareQueue("d1.t3");
                imqd.queueBind("*.*.12", "d1.t3");
                IMQReadOnly imqr = imqd.getQueue("d1.t3");
                if (!"123".equals(imqr.mqRead())) reuslt.add("123");
                if (!"456".equals(imqr.mqRead())) reuslt.add("456");
                if (!"789".equals(imqr.mqRead())) reuslt.add("789");
                if (imqr.mqRead() != null) reuslt.add("t3 not null");
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        Assertions.assertEquals(0, reuslt.size());
    }

    @Test
    public void partAnyString_atLastSlot_topic() throws InterruptedException {
        List<String> reuslt = new ArrayList<>();

        MQManager mqm = new MQManager();

        Thread t1 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                if (!mqm.getDispatcher("d1").put("1.a.12", "123")) reuslt.add("1a12");
                if (!mqm.getDispatcher("d1").put("2.b.12", "456")) reuslt.add("2b12");
                if (!mqm.getDispatcher("d1").put("1.a.15", "789")) reuslt.add("1a15");
                mqm.getDispatcher("d1").release(null);
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        Thread t2 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);
                
            try {
                IMQDispatcher imqd = mqm.getDispatcher("d1");
                imqd.declareQueue("d1.t2");
                imqd.queueBind("1.a.*", "d1.t2");
                IMQReadOnly imqr = imqd.getQueue("d1.t2");
                if (!"123".equals(imqr.mqRead())) reuslt.add("123");
                if (!"789".equals(imqr.mqRead())) reuslt.add("789");
                if (imqr.mqRead() != null) reuslt.add("t2 not null");
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        Thread t3 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);
                
            try {
                IMQDispatcher imqd = mqm.getDispatcher("d1");
                imqd.declareQueue("d1.t3");
                imqd.queueBind("*.*.*", "d1.t3");
                IMQReadOnly imqr = imqd.getQueue("d1.t3");
                if (!"123".equals(imqr.mqRead())) reuslt.add("123");
                if (!"456".equals(imqr.mqRead())) reuslt.add("456");
                if (!"789".equals(imqr.mqRead())) reuslt.add("789");
                if (imqr.mqRead() != null) reuslt.add("t3 not null");
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        Assertions.assertEquals(0, reuslt.size());
    }

    @Test
    public void anyString_afterDot_topic() throws InterruptedException {
        List<String> reuslt = new ArrayList<>();

        MQManager mqm = new MQManager();

        Thread t1 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                if (!mqm.getDispatcher("d1").put("1.a.12", "123")) reuslt.add("1a12");
                if (!mqm.getDispatcher("d1").put("2.b.12", "456")) reuslt.add("2b12");
                if (!mqm.getDispatcher("d1").put("1.a.15", "789")) reuslt.add("1a15");
                mqm.getDispatcher("d1").release(null);
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        Thread t2 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);
                
            try {
                IMQDispatcher imqd = mqm.getDispatcher("d1");
                imqd.declareQueue("d1.t2");
                imqd.queueBind("1.#", "d1.t2");
                IMQReadOnly imqr = imqd.getQueue("d1.t2");
                if (!"123".equals(imqr.mqRead())) reuslt.add("123");
                if (!"789".equals(imqr.mqRead())) reuslt.add("789");
                if (imqr.mqRead() != null) reuslt.add("t2 not null");
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        Thread t3 = new Thread(() -> {
            mqm.declareDispatcher("d1", MQType.TOPIC);
                
            try {
                IMQDispatcher imqd = mqm.getDispatcher("d1");
                imqd.declareQueue("d1.t3");
                imqd.queueBind("*.#", "d1.t3");
                IMQReadOnly imqr = imqd.getQueue("d1.t3");
                if (!"123".equals(imqr.mqRead())) reuslt.add("123");
                if (!"456".equals(imqr.mqRead())) reuslt.add("456");
                if (!"789".equals(imqr.mqRead())) reuslt.add("789");
                if (imqr.mqRead() != null) reuslt.add("t3 not null");
            } catch (UndeclaredException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        Assertions.assertEquals(0, reuslt.size());
    }

}
