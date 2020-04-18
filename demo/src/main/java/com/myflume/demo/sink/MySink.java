package com.myflume.demo.sink;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * program: flume-study->MySink
 * description:自定义Flume Sink
 * author: gerry
 * created: 2020-04-17 10:29
 **/
public class MySink extends AbstractSink implements Configurable {

    //记录日志
    private static final Logger logger = LoggerFactory.getLogger(AbstractSink.class);

    // 标记
    private boolean flag = true;


    @Override
    public Status process() throws EventDeliveryException {
        //初始化Status
        Status status = Status.READY;
        Transaction trans = null;
        try {
            //开始事务
            Channel channel = getChannel();
            trans = channel.getTransaction();

            trans.begin();

            //获取Event
            Event event = channel.take();
//            while (event != null) {
//                //获取body
//                String body = new String(event.getBody());
//                System.out.println(body);
//            }

            while (flag) {
                event = channel.take();
                if (event != null) {
                    break;
                }
            }

            //获取body
            String body = new String(event.getBody(), StandardCharsets.UTF_8);
            logger.info("sink get body content:{}", body);


            if (event == null) {
                status = Status.BACKOFF;
            }

            trans.commit();
        } catch (Exception e) {
            //有异常的时候还是要提交
            if (trans != null) {
                trans.commit();
            }
            e.printStackTrace();
        } finally {
            if (trans != null) {
                trans.close();
            }
        }

        return status;
    }

    @Override
    public void configure(Context context) {

    }

    @Override
    public synchronized void start() {
        logger.debug("com.myflume.sink.MySink to start");
        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.debug("com.myflume.sink.MySink to stop");
        setFlag(false);
        super.stop();
    }

    // 开关标记
    public void setFlag(boolean flag) {
        this.flag = flag;
    }
}
