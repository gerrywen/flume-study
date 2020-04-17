package com.myflume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

/**
 * program: flume-study->MySource
 * description: 自定义Flume Source
 *
 * 自定义的消息有两种类型的Source：
 *   1.PollableSource （轮训拉取）,主动拉取消息
 *   2.EventDrivenSource （事件驱动）,被动等待
 *
 * author: gerry
 * created: 2020-04-17 10:30
 **/
public class MySource extends AbstractSource implements Configurable, PollableSource {
    @Override
    public Status process() throws EventDeliveryException {
        return null;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {

    }
}
