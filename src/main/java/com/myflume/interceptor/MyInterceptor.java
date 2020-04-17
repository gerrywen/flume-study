package com.myflume.interceptor;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * program: flume-study->MyInterceptor
 * description: 自定义Flume Interceptor
 * author: gerry
 * created: 2020-04-17 10:28
 **/
public class MyInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        return null;
    }

    @Override
    public void close() {

    }
}
