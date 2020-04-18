package com.myflume.demo.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * program: flume-study->MyInterceptor
 * description: 自定义Flume Interceptor
 * author: gerry
 * created: 2020-04-17 10:28
 **/
public class MyInterceptor implements Interceptor {

    //记录日志
    private static final Logger logger = LoggerFactory.getLogger(MyInterceptor.class);

    @Override
    public void initialize() {

    }

    /**
     * 对单个Event内部的处理
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), StandardCharsets.UTF_8);
        logger.info("intercept get body content:{}", body);
        StringBuffer bodyString = new StringBuffer();
        //当发现body以test开头时，将body替换成123456
        if (body.startsWith("test")) {
            bodyString = bodyString.append("123456");
            event.setBody(bodyString.toString().getBytes());
        }

        return event;
    }

    /**
     * 对多个Event的处理
     *
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        //reset Event
        for (Event event : events) {
            if (event != null) intercept(event);
        }

        return events;
    }

    @Override
    public void close() {

    }


    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
