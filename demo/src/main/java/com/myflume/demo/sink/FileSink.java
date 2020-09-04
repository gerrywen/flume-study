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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * program: flume-study->FileSink
 * description: 文件sink
 * author: gerry
 * created: 2020-08-12 15:43
 **/
public class FileSink extends AbstractSink implements Configurable {
    //记录日志
    private static final Logger logger = LoggerFactory.getLogger(AbstractSink.class);

    private static final String PROP_KEY_ROOTPATH = "fileName";

    private String fileName;

    // 标记
    private boolean flag = true;


    @Override
    public void configure(Context context) {
        logger.info("sink read configure start >>>> {}", context.toString());
        this.fileName = context.getString(PROP_KEY_ROOTPATH);
        logger.info("sink read configure end >>>> ");
    }

    @Override
    public Status process() throws EventDeliveryException {

        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        Event event = null;
        txn.begin();
        while (this.flag) {
            event = ch.take();
            if (event != null) {
                break;
            }
        }

        try {
            logger.debug("sink get event .");
            String body = new String(event.getBody());
            logger.info("event.getBody() >>>>> " + body);

            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            String currentTime = sdf.format(date);
//            String res = "当前时间：" + currentTime + "\n" + body + "\r\n";
            String res = body + "\r\n";
//            logger.info("fileName>>>>> " + this.fileName);
            File file = new File(this.fileName);
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    logger.error("fileSink createNewFile IOException", e);
                }
            }
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(file, true);
            } catch (FileNotFoundException e) {
                logger.error("fileSink FileNotFoundException", e);
            }

            try {
                fos.write(res.getBytes());
            } catch (IOException e) {
                logger.error("fileSink write IOException", e);
            }

            try {
                fos.close();
            } catch (IOException e) {
                logger.error("fileSink close IOException", e);
            }
            txn.commit();
            return Status.READY;
        } catch (Throwable th) {
            txn.rollback();
            if (th instanceof Error) {
                throw (Error) th;
            } else {
                throw new EventDeliveryException(th);
            }
        } finally {
            txn.close();
        }
    }

    @Override
    public synchronized void start() {
        logger.debug("com.myflume.sink.FileSink to start : " + new Date());
        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.debug("com.myflume.sink.FileSink to stop : " + new Date());
        setFlag(false);
        super.stop();
    }

    // 开关标记
    public void setFlag(boolean flag) {
        this.flag = flag;
    }
}
