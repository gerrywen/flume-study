package com.myflume.sink;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

/**
 * program: flume-study->MySink
 * description:自定义Flume Sink
 * author: gerry
 * created: 2020-04-17 10:29
 **/
public class MySink extends AbstractSink implements Configurable {
    @Override
    public Status process() throws EventDeliveryException {
        return null;
    }

    @Override
    public void configure(Context context) {

    }
}
