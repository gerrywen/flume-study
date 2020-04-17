package com.myflume.source;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * program: flume-study->TailFileSource
 * description: 自定义source，记录偏移量
 * flume的生命周期： 先执行构造器，再执行 config方法 -> start方法-> processor.process
 * 读取配置文件:(配置读取的文件内容：读取那个文件，编码及、偏移量写到那个文件，多长时间检测一下文件是否有新内容
 * author: gerry
 * created: 2020-04-17 10:49
 **/
public class TailFileSource extends AbstractSource implements EventDrivenSource, Configurable {

    //记录日志
    private static final Logger logger = LoggerFactory.getLogger(TailFileSource.class);

    // 文件路径
    private String filePath;

    // 默认UTF-8编码
    private String charset;

    //偏移量写入位置
    private String positionFile;

    // 定时时间
    private long interval;

    // 创建线程池
    private ExecutorService executor;

    // 读写文件线程
    private FileRunnable fileRunnable;

    @Override
    public void configure(Context context) {
        //读取哪个文件
        filePath = context.getString("filePath");
        //默认使用utf-8
        charset = context.getString("charset", "UTF-8");
        //把偏移量写到哪
        positionFile = context.getString("positionFile");
        //指定默认每个一秒 去查看一次是否有新的内容
        interval = context.getLong("interval", 1000L);
    }

    @Override
    public synchronized void start() {
        // 创建一个单线程的线程池
        executor = Executors.newCachedThreadPool();
        //获取一个ChannelProcessor
        final ChannelProcessor channelProcessor = getChannelProcessor();
        fileRunnable = new FileRunnable(filePath, charset, positionFile, interval, channelProcessor);
        //提交到线程池中
        executor.submit(fileRunnable);
        //调用父类的方法
        super.start();
    }

    @Override
    public synchronized void stop() {
        //停止
        fileRunnable.setFlag(false);
        //停止线程池
        executor.shutdown();
        while (!executor.isTerminated()) {
            logger.debug("Waiting for filer exec executor service to stop");
            try {
                //等500秒在停
                executor.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.debug("InterutedExecption while waiting for exec executor service" +
                        " to stop . Just exiting");
                e.printStackTrace();
            }
        }
        super.stop();
    }

    private static class FileRunnable implements Runnable {
        // 默认UTF-8编码
        private String charset;
        // 间隔时间
        private long interval;
        // 偏移量
        private long offset = 0L;
        // flume通道进程
        private ChannelProcessor channelProcessor;
        // 实现高效多线程读写
        private RandomAccessFile raf;
        // 标记
        private boolean flag = true;
        // 文件路径
        private String filePath;
        // 读取文件偏移量
        private File posFile;

        /**
         * 先于run方法执行，构造器只执行一次
         * 先看看有没有偏移量，如果有就接着读，如果没有就从头开始读
         *
         * @param filePath
         * @param charset
         * @param positionFile
         * @param interval
         * @param channelProcessor
         */
        public FileRunnable(String filePath, String charset, String positionFile,
                            long interval, ChannelProcessor channelProcessor) {

            this.charset = charset;
            this.interval = interval;
            this.channelProcessor = channelProcessor;
            this.filePath = filePath;
            //读取偏移量， 在postionFile文件
            posFile = new File(positionFile);
            try {
                if (!posFile.exists()) {
                    //如果不存在就创建一个文件
                    posFile.createNewFile();
                }
                // 读取文件字符串
                String offsetString = FileUtils.readFileToString(posFile);
                //以前读取过
                if (!offsetString.isEmpty() && null != offsetString && !"".equals(offsetString)) {
                    //把偏移量穿换成long类型
                    offset = Long.parseLong(offsetString);
                }
            } catch (Exception e) {
                logger.error("运行FileRunnable失败>>>>>>>>:", e);
            }
        }

        @Override
        public void run() {
            // 开启线程一直运行
            while (flag) {
                //按照指定的偏移量读取数据
                // List<Event> events = new ArrayList<Event>();
                //读取文件中的新数据
                try {
                    // 写文件
                    raf = new RandomAccessFile(filePath, "r");
                    //按照指定的偏移量读取
                    raf.seek(offset);
                    String line = raf.readLine();
                    logger.info("file content:{}", line);
                    if (line != null) {
                        //有数据进行处理，避免出现乱码
                        line = new String(line.getBytes(StandardCharsets.UTF_8), charset);
                        channelProcessor.processEvent(EventBuilder.withBody(line.getBytes()));
                        //获取偏移量,更新偏移量
                        offset = raf.getFilePointer();
                        //将偏移量写入到位置文件中
                        FileUtils.writeStringToFile(posFile, offset + "");
                    } else {
                        //没读到😴一会儿
                        Thread.sleep(interval);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.error("read filethread Interrupted", e);
                } catch (IOException e) {
                    logger.error("read log file error", e);
                }
            }
        }

        // 开关标记
        public void setFlag(boolean flag) {
            this.flag = flag;
        }
    }
}
