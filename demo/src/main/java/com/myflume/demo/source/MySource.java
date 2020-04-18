package com.myflume.demo.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Random;

/**
 * program: flume-study->MySource
 * description: 自定义Flume Source
 * <p>
 * 自定义的消息有两种类型的Source：
 * 1.PollableSource （轮训拉取）,主动拉取消息
 * 2.EventDrivenSource （事件驱动）,被动等待
 * <p>
 * author: gerry
 * created: 2020-04-17 10:30
 **/
public class MySource extends AbstractSource implements Configurable, PollableSource {

    //记录日志
    private static final Logger logger = LoggerFactory.getLogger(AbstractSource.class);

    @Override
    public Status process() throws EventDeliveryException {
        //生成0-10的随机数,组合成一个text
        Random random = new Random();
        int randomNum = random.nextInt(10);
        String text = "myfirstSource" + randomNum;
        //生成Header
        HashMap<String, String> header = new HashMap<String, String>();
        header.put("id", Integer.toString(randomNum));

        //EventBuilder.withBody:将给定的header和body组合成Event，并制定字符集
        //getChannelProcessor.processEvent():将给定的Event put到每个配置的Channel
        this.getChannelProcessor().processEvent(EventBuilder.withBody(text, StandardCharsets.UTF_8, header));
        //Ready状态表示Event可以被取走，还有一个状态是Backoff，表示让Flume睡眠一段时间
        return Status.READY;
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
