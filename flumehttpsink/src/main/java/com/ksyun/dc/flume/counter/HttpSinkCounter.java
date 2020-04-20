package com.ksyun.dc.flume.counter;

import org.apache.flume.instrumentation.SinkCounter;

/**
 * program: flume-study->HttpSinkCounter
 * description:
 * <p>
 * Flume-NG内置计数器(监控)源码级分析：
 * https://www.cnblogs.com/lxf20061900/p/3845356.html
 * <p>
 * author: gerry
 * created: 2020-04-19 22:47
 **/
public class HttpSinkCounter extends SinkCounter implements HttpSinkCounterMBean {

    /**
     * 自定义统计发送时间
     */
    private static final String TIME_SINK_EVENT_SEND = "sink.event.send.time";
    /**
     * 自定义统计错误回滚次数
     */
    private static final String COUNT_ROLLBACK = "sink.rollback.count";

    /**
     * 自定义数据统计
     */
    private static final String[] ATTRIBUTES = {COUNT_ROLLBACK, TIME_SINK_EVENT_SEND};

    /**
     * 必须实现构造方法，增加自定义统计属性
     * @param name
     */
    public HttpSinkCounter(String name) {
        super(name, ATTRIBUTES);
    }

    /**
     * 增加错误回滚统计数量
     * @return
     */
    public long incrementRollbackCount() {
        return increment(COUNT_ROLLBACK);
    }


    /**
     * 增加flume传输事件时间统计
     * @param delta
     * @return
     */
    public long addToHttpEventSendTimer(long delta) {
        return addAndGet(TIME_SINK_EVENT_SEND, delta);
    }

    /**
     * 获取时间统计个数
     * @return
     */
    @Override
    public long getHttpEventSendTimer() {
        return get(TIME_SINK_EVENT_SEND);
    }

    /**
     * 获取回滚数量统计
     * @return
     */
    @Override
    public long getRollbackCount() {
        return get(COUNT_ROLLBACK);
    }
}
