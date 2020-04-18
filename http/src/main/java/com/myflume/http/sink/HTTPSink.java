package com.myflume.http.sink;

import com.alibaba.fastjson.JSONArray;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * program: flume-study->HTTPSink
 * description: 自定义http sink
 * <p>
 * 将sink数据下沉发送给NGINX
 * <p>
 * <p>
 * Apache-Flume日志收集+自定义HTTP Sink处理 测试用例搭建:
 * https://blog.csdn.net/kkillala/article/details/82155845
 * <p>
 * author: gerry
 * created: 2020-04-18 23:22
 **/
public class HTTPSink extends AbstractSink implements Configurable {
    // 日志打印
    private static final Logger logger = LoggerFactory.getLogger(HTTPSink.class);

    // 处理sink发送的http地址
    private String hostname;
    // 处理sink发送的http端口
    private String port;
    // 处理sink发送的http地址
    private String url;
    // 批量大小
    private int batchSize;
    // 记录请求地址
    private String postUrl;

    /**
     * 构造方法
     */
    public HTTPSink() {
        System.out.println("HTTPSink start...");
    }

    /**
     * 主业务逻辑处理
     *
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        // 事物
        Transaction tx = null;
        // 状态
        Status status = null;
        // 数据传输管道
        Channel channel = getChannel();
        try {
            // 开启事物
            tx = channel.getTransaction();
            tx.begin();

            logger.info("开始channel batch 提交");
            for (int i = 0; i < batchSize; i++) {
                // 使用take方法尽可能的以批量的方式从Channel中读取事件，直到没有更多的事件
                Event event = channel.take();
                if (event == null) {
                    break;
                } else {  // 也可以不需要else
                    byte[] body = event.getBody();
                    String str = new String(body);
                    logger.info("=======================================" + str);
                    Response response = postJson(postUrl, JSONArray.toJSON(str).toString());
                    logger.info(">>>>>>>>>>>>>>>>>>> HTTPSink send post response : {}", response);
                }
            }
            logger.info("channel batch 完成提交");
            // 提交事物
            tx.commit();
            status = Status.READY;
        } catch (Exception e) {
            logger.error("HTTPSink process catch Exception {}", e.getMessage());
            if (tx != null) {
                tx.commit();// commit to drop bad event, otherwise it will enter dead loop.
            }
            e.printStackTrace();
        } finally {
            if (tx != null) {
                try {
                    // 关闭事物
                    tx.close();
                } catch (Exception e) {
                    logger.error("HTTPSink process finally Exception {}", e.getMessage());
                    tx.commit();
                    tx.close();
                }
            }
        }
        return status;
    }

    public Status process4() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        try {
            transaction = channel.getTransaction();
            transaction.begin();
            Event event = null;
            String content = null;
            List<String> contents = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {//对事件进行处理
                    content = new String(event.getBody());
                    contents.add(content);
                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }
            if (contents.size() > 0) {
                Response response = postJson(postUrl, JSONArray.toJSON(contents).toString());
                if (response != null && response.isSuccessful()) {
                    transaction.commit();//通过 commit 机制确保数据不丢失
                }
            } else {
                transaction.commit();
            }
        } catch (Exception e) {
            try {
                transaction.rollback();
            } catch (Exception e2) {
                logger.error("Exception in rollback. Rollback might not have been" +
                        "successful.");
            }
            logger.error("Failed to commit transaction." +
                    "Transaction rolled back.");
            Throwables.propagate(e);
        } finally {
            if (transaction != null) {
//              transaction.commit();
                transaction.close();
                logger.info("close Transaction");
            }
        }
        logger.info("HTTPSink ..." + result);
        return result;
    }

    /**
     * 读取flume avroSinks配置信息
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        logger.info("Begin read configuration context {}", context);
        hostname = context.getString("hostname");
        Preconditions.checkNotNull(hostname, "hostname must be set!!");
        port = context.getString("port");
        Preconditions.checkNotNull(port, "port must be set!!");
        url = context.getString("url");
        Preconditions.checkNotNull(url, "url must be set!!");
        batchSize = context.getInteger("batchSize", 100);
        postUrl = "http://" + hostname + ":" + port + url;
        logger.info("postURL..." + postUrl);
    }

    @Override
    public void start() {
        super.start();
        logger.info("LogCollector start...");
    }

    @Override
    public void stop() {
        super.stop();
        logger.info("LogCollector stop...");
    }

    /**
     * post请求，json数据为body
     *
     * @param url  请求地址
     * @param json 字符串
     * @return
     */
    private Response postJson(String url, String json) {
        OkHttpClient client = new OkHttpClient();
        //String转RequestBody String、ByteArray、ByteString都可以用toRequestBody()
//        MediaType mediaType = MediaType.Companion.parse("application/json;charset=utf-8");
//        RequestBody body = RequestBody.Companion.create(json, mediaType);
        RequestBody body = RequestBody.create(MediaType.parse("application/json"), json);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        Response response = null;
        try {
            response = client.newCall(request).execute();
            if (!response.isSuccessful()) {
                logger.error("request was error {}", response);
            }
        } catch (IOException e) {
            logger.error("request was error {}", e.getMessage());
            e.printStackTrace();
        }
        return response;
    }

}
