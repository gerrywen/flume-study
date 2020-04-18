package com.myflume.http.handler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import java.io.BufferedReader;
import java.lang.reflect.Type;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.flume.source.http.HTTPBadRequestException;

/**
 * program: flume-study->HTTPSourceAuthTokenHandler
 * description: 自定义handler
 * 处理HTTP数据请求，授权验证
 * author: gerry
 * created: 2020-04-18 21:39
 * <p>
 * flume架构event解析:
 * Flume采用事务来保证数据传输的可靠性。
 * 每个event只有当被确认传递到下游节点或者数据沉积池之后才把该event从channel中删除。
 * 节点中的sources和sinks被包含在一个事务中。
 * source接收上游的events，若其中一个event接收异常，则所有的events都丢弃，这将导致数据重发。
 * 同理，sink在将events导入到下游数据沉积池时，任何一个event发送异常，则所有的events都将重新发送。
 *
 * <p>
 * Request Example
 * [{"headers" : {"auth":"YOUR_TOKEN", "some-header": "some-value"},"body": "random_body1"},
 * {"headers" : {"auth": "YOUR_TOKEN"},"body": "random_body2"}]
 **/
public class HTTPSourceAuthTokenHandler implements HTTPSourceHandler {
    // 日志打印
    private static final Logger logger = LoggerFactory.getLogger(HTTPSourceAuthTokenHandler.class);

    // 读取flume配置文件
    private final String CONF_INSERT_AUTHTOKEN = "authtoken";

    // 读取flume配置文件
    private final String CONF_INSERT_TIMESTAMP = "insertTimestamp";

    // 获取配置token
    private String authToken;

    // 标记配置是否开启时间戳
    private boolean insertTimestamp;

    // 放入当前请求时间戳到头部
    private final String TIMESTAMP_HEADER = "timestamp";

    // 根据泛型返回解析制定的类型
    private final Type listType =
            new TypeToken<List<JSONEvent>>() {
            }.getType();

    private final Gson gson;


    // 构造方法
    public HTTPSourceAuthTokenHandler() {
        // 禁用html转义
        gson = new GsonBuilder().disableHtmlEscaping().create();
    }


    @Override
    public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception {
        logger.info("Begin get events >>>>>>>>>>");
        BufferedReader reader = request.getReader();
        String charset = request.getCharacterEncoding();
        //UTF-8 is default for JSON. If no charset is specified, UTF-8 is to
        //be assumed.
        if (charset == null) {
            logger.info("Charset is null, default charset of UTF-8 will be used.");
            charset = "UTF-8";
        } else if (!(charset.equalsIgnoreCase("utf-8")
                || charset.equalsIgnoreCase("utf-16")
                || charset.equalsIgnoreCase("utf-32"))) {
            logger.error("Unsupported character set in request {}. "
                    + "JSON handler supports UTF-8, "
                    + "UTF-16 and UTF-32 only.", charset);
            throw new UnsupportedCharsetException("JSON handler supports UTF-8, "
                    + "UTF-16 and UTF-32 only.");
        }

        /*
         * Gson throws Exception if the data is not parseable to JSON.
         * Need not catch it since the source will catch it and return error.
         */
        List<Event> eventList = new ArrayList<Event>(0);
        try {
            // 格式转换
            eventList = gson.fromJson(reader, listType);
        } catch (JsonSyntaxException ex) {
            throw new HTTPBadRequestException("Request has invalid JSON Syntax.", ex);
        }
        for (Event e : eventList) {
            ((JSONEvent) e).setCharset(charset);
        }
        return getSimpleEvents(eventList);
    }

    /**
     * 读取配置文件信息
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        logger.info("Begin read configuration context {}", context);
        authToken = context.getString(CONF_INSERT_AUTHTOKEN, "ccbtoken");
        insertTimestamp = context.getBoolean(CONF_INSERT_TIMESTAMP, false);
        logger.info("Configuration Auth Token: " + authToken);
        logger.info("Configuration insertTimestamp: " + insertTimestamp);
    }

    /**
     * flume 数据传输列表
     *
     * @param events
     * @return
     */
    private List<Event> getSimpleEvents(List<Event> events) {
        // Flume NG传输的数据的基本单位是event，如果是文本文件，通常是一行记录，这也是事务的基本单位。
        // 一个event由包含数据的有效负载(payload)和一组可选属性组成
        List<Event> newEvents = new ArrayList<>(events.size());
        events.forEach(e -> {
            boolean isValid = false;
            // 获取输入流头部信息
            Map<String, String> eventHeaders = e.getHeaders();
            // 判断头部是否传递授权信息
            if (eventHeaders.size() == 0 || eventHeaders.get("auth") == null) {
                throw new HTTPBadRequestException("Must request Authorisation in Header.");
            }
            // 判断请求头里面是否有auth信息
            if (eventHeaders.get("auth").contains(authToken)) {
                logger.info("Request has valid authorisation header.");
            } else {
                logger.info("Request has invalid authorisation header.");
                logger.info("Header Value: " + eventHeaders.get("auth"));
                throw new HTTPBadRequestException("Request has invalid Authorisation Header.");
            }
            // 头部放置当前服务器时间戳
            if (insertTimestamp) {
                eventHeaders.put(TIMESTAMP_HEADER, String.valueOf(System
                        .currentTimeMillis()));
            }
            newEvents.add(EventBuilder.withBody(e.getBody(), eventHeaders));
        });
        return newEvents;
    }
}
