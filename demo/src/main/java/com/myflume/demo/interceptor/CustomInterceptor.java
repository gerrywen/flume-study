package com.myflume.demo.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * program: flume-study->CustomInterceptor
 * description: 创建自定义拦截器
 * https://www.cnblogs.com/jhxxb/p/11582804.html
 * author: gerry
 * created: 2020-04-17 16:06
 **/
public class CustomInterceptor implements Interceptor {

    //记录日志
    private static final Logger logger = LoggerFactory.getLogger(CustomInterceptor.class);

    @Override
    public void initialize() {

    }

    // 单个事件拦截
    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        logger.info("intercept get body content:{}", body);
        if (body[0] < 'z' && body[0] > 'a') {
            // 自定义头信息
            event.getHeaders().put("type", "letter");
        } else if (body[0] > '0' && body[0] < '9') {
            // 自定义头信息
            event.getHeaders().put("type", "number");
        }
        return event;
    }

    // 批量事件拦截
    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }


    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new CustomInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
