package com.ksyun.dc.flume.counter;

/**
 * program: flume-study->HttpSinkCounterMBean
 * description:
 * author: gerry
 * created: 2020-04-19 22:47
 **/
public interface HttpSinkCounterMBean {
    long getHttpEventSendTimer();

    long getRollbackCount();

    long getConnectionCreatedCount();

    long getConnectionClosedCount();

    long getConnectionFailedCount();

    long getBatchEmptyCount();

    long getBatchUnderflowCount();

    long getBatchCompleteCount();

    long getEventDrainAttemptCount();

    long getEventDrainSuccessCount();

    long getStartTime();

    long getStopTime();

    String getType();

}
