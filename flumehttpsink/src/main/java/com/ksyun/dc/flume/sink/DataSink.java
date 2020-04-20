package com.ksyun.dc.flume.sink;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ksyun.dc.flume.counter.HttpSinkCounter;
import com.ksyun.dc.flume.utils.HttpRequest;
import com.ksyun.dc.flume.utils.SecurityUtil;
import com.ksyun.dc.flume.utils.TimeUtils;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

/**
 * program: flume-study->DataSink
 * description: httpsink数据下沉
 * author: gerry
 * created: 2020-04-19 23:16
 **/
public class DataSink extends AbstractSink implements Configurable {
    // 日志打印
    private static final Logger logger = LoggerFactory.getLogger(DataSink.class);

    private static PoolingClientConnectionManager cm;

    private HttpSinkCounter counter;

    public static final int MAX_TOTAL_CONNECTIONS = 20000;

    public static final int MAX_ROUTE_CONNECTIONS = 100;

    public static ObjectMapper objectMapper = new ObjectMapper();

    private static String url;

    private int batchSize;

    private int minBatchSize;

    private int threadNum;

    private ExecutorService executor;

    private String principalkey;

    private String principalPassword;

    private String comresssEanble;

    private int sendTimeout;

    private boolean needEliminateDuplication;

    private int connectTimeout;

    private int processDelay;

    private String[] ips;

    private String domain;

    private String onlineUrl;

    private static Long changeUrlStartTime = 0L;

    private Long changeUrlTimeout = 60000L;

    private Long maxBatchBytes;

    private final AtomicInteger currentThreads = new AtomicInteger(0);

    private boolean isOnlineUrl = true;

    private String tenantId;

    private String userId;

    private String projectId;

    private String topicName;

    private String errTopicName;

    private String env;

    private String ownerProjectId;

    private String errOwnerProjectId;

    private String kafkaSourceId;

    private String appId;

    private String appName;

    private String requestMethod;

    private String projectName;

    private String algorithm;

    private String region;

    private String service;

    private String signedHeaders;

    private String host;

    private String method;

    private String canonical_uri;

    private String canonical_querystring;

    private String content_type;

    private String credentialData;

    private String credential;

    private String amzdate;

    /**
     * 是否开启Schema检核开关 默认为true
     */
    private Boolean checkTopicSchema;

    @Override
    public synchronized void start() {
        this.counter.start();
        logger.info("Http sink {} start. Metrics: {}", getName(), this.counter);
        super.start();
    }

    @Override
    public synchronized void stop() {
        while (this.currentThreads.get() > 0) {
            logger.info("ready to stop http sink , current threads : " + this.currentThreads.get());
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.counter.stop();
        logger.info("Http sink {} start. Metrics: {}", getName(), this.counter);
        super.stop();
    }

    public static void initPool() {
        initPool(20000, 100);
    }

    public static void initPool(int maxTotalConnections, int maxPerRoute) {
        cm.setMaxTotal(maxTotalConnections);
        cm.setDefaultMaxPerRoute(maxPerRoute);
    }

    /**
     * 未知host异常捕捉
     *
     * @param responseJson
     * @return
     */
    public static boolean isUnknownHostException(JSONObject responseJson) {
        return (responseJson != null) && (responseJson.size() > 0)
                && (responseJson.has("errorCode")) && (responseJson.getInt("errorCode") == 6001);
    }

    public static JSONObject doHttpPost(String comresssEanble, String url, String messages, byte[] payloadByteArray,
                                        Map<String, String> headers, int readTimeout, int conTimeout, boolean needEliminateDuplication) {
        JSONObject responseJson = new JSONObject();
        if (!needEliminateDuplication) {
            responseJson = postData(comresssEanble, url, messages, payloadByteArray, headers, readTimeout, conTimeout);
        } else {
            UUID uuid = UUID.randomUUID();
            headers.put("uuid", uuid.toString());
            long serialId = 0L;
            while (true) {
                headers.put("serial-id", String.valueOf(serialId));
                responseJson = postData(comresssEanble, url, messages, payloadByteArray, headers, readTimeout, conTimeout);
                if (serialId > 128L) {
                    logger.warn("response is {}, retry too many, uuid is {}", responseJson.toString(), uuid);
                    break;
                }

                if ((responseJson.size() > 0) && (responseJson.getInt("code") == 200)) {
                    break;
                }

                if ((responseJson.size() > 0) && (responseJson.getInt("code") == 401)) {
                    logger.warn(responseJson.toString());
                    break;
                }
                if (isUnknownHostException(responseJson)) {
                    logger.warn("response is {}, uuid is {}", responseJson.toString(), uuid);
                    break;
                }
                serialId += 1L;
                logger.info("http timeout, need retry, uuid is {}, serial-id is {}", uuid, serialId);
                logger.info("response is {}", responseJson.toString());
                try {
                    Thread.sleep(serialId);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

        return responseJson;
    }

    public static JSONObject postData(String comresssEanble, String url, String messages, byte[] payloadByteArray,
                                      Map<String, String> headers, int readTimeout, int conTimeout) {
        JSONObject responseJson = new JSONObject();

        logger.info(url + "  " + headers);
        try {
            if ((comresssEanble != null) && (comresssEanble.equals("true"))) {
                headers.put("Content-Encoding", "gzip");
                responseJson = HttpRequest.doPostHttps(url, payloadByteArray, headers, readTimeout, conTimeout);
            } else {
                responseJson = HttpRequest.doPostHttps(url, messages, headers, readTimeout, conTimeout);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return responseJson;
    }

    /**
     * 获取回调地址
     *
     * @return
     */
    public String getBackUrl() {
        String url = this.onlineUrl;
        if ((this.ips != null) && (this.ips.length >= 1)) {
            Random random = new Random();
            int s = random.nextInt(this.ips.length);
            url = this.onlineUrl.replace(this.domain, this.ips[s].trim());
        }
        return url;
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        final Channel channel = getChannel();
        final AtomicInteger noDataTimes = new AtomicInteger(0);
        int newThread;
        for (newThread = 0; newThread < this.threadNum; newThread++) {
            if (this.processDelay > 0) {
                try {
                    Thread.sleep(this.processDelay / this.threadNum);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if (this.currentThreads.get() >= this.threadNum) {
                break;
            }
            this.executor.execute(new Runnable() {
                @Override
                public void run() {
                    DataSink.this.sinkTask(channel, noDataTimes);
                }
            });
            this.currentThreads.getAndIncrement();
        }
        if (newThread == 0) {
            try {
                logger.info("no thread create, sleep 200ms");
                Thread.sleep(this.processDelay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else if (noDataTimes.get() > 0) {
            logger.info("data is too less: waiting......");
            result = Status.BACKOFF;
        }
        return result;
    }

    /**
     * 读取flume配置文件
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        logger.info("Begin read configuration context {}", context);

        url = context.getString("endpoint");
        this.principalkey = context.getString("accessId");
        this.principalPassword = context.getString("accessKey");
        try {
            byte[] base64decodeBytes = Base64.getDecoder().decode(this.principalPassword);
            this.principalPassword = new String(base64decodeBytes, StandardCharsets.UTF_8);
            this.principalPassword = SecurityUtil.decrypt(this.principalPassword, this.principalkey);
        } catch (Exception e) {
            logger.error(e.toString());
        }

        this.tenantId = context.getString("tenantId");
        this.userId = context.getString("userId");
        this.projectId = context.getString("projectId");
        this.topicName = context.getString("topicName");
        this.errTopicName = context.getString("errTopicName");
        this.env = context.getString("runEnvironment");
        this.ownerProjectId = context.getString("ownerProjectId");
        this.errOwnerProjectId = context.getString("errOwnerProjectId");
        this.kafkaSourceId = context.getString("kafkaSourceId");
        this.appId = context.getString("appId");
        this.appName = context.getString("appName");
        this.requestMethod = context.getString("requestMethod");
        this.projectName = context.getString("projectName");
        this.algorithm = context.getString("algorithm", "AWS4-HMAC-SHA256");
        this.region = context.getString("region", "cn-shanghai-3");
        this.service = context.getString("service", "datacloud");
        this.signedHeaders = context.getString("signedHeaders", "content-type;host;x-amz-date");
        this.host = context.getString("host");
        this.method = context.getString("method", "POST");
        this.canonical_uri = context.getString("canonical_uri", "/");
        this.canonical_querystring = url.split("\\?")[0];
        this.content_type = context.getString("content_type", "text/plain");
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date();
        this.credentialData = df.format(date);
        this.credential = (this.principalkey + "/" + this.credentialData + "/" + this.region + "/" + this.service + "/aws4_request");
        this.amzdate = TimeUtils.getWordTime(date);

        this.onlineUrl = url;
        this.domain = StringUtils.split(this.onlineUrl, "/")[1];
        this.batchSize = context.getInteger("batchMaxSize", 500);
        this.minBatchSize = context.getInteger("batchMinSize", 20);
        this.threadNum = context.getInteger("threadNum", 5);


        this.comresssEanble = context.getString("compress", "true");
        this.processDelay = context.getInteger("processDelay", 200);
        this.sendTimeout = context.getInteger("sendTimeout", 30000);
        this.connectTimeout = context.getInteger("connectTimeout", 30000);
        this.needEliminateDuplication = context.getBoolean("needEliminateDuplication", true);
        this.checkTopicSchema = context.getBoolean("checkTopicSchema", true);
        if (context.getString("ips") != null) {
            this.ips = context.getString("ips").split(",");
        }
        this.changeUrlTimeout = context.getLong("changeUrlTimeout", 60000L);
        this.maxBatchBytes = context.getLong("maxBatchBytes", 5242880L);

        this.executor = Executors.newCachedThreadPool();
        if (this.counter == null) {
            this.counter = new HttpSinkCounter(getName());
        }
    }

    /**
     * sink提交任务
     *
     * @param channel     消息传输通道
     * @param noDataTimes 原子性统计数
     */
    private void sinkTask(Channel channel, AtomicInteger noDataTimes) {
        // 事物提交
        Transaction transaction = null;
        long takedEventSize = 0L;
        long takedEventBytes = 0L;
        try {
            // 获取事物
            transaction = channel.getTransaction();
            // 开启事物
            transaction.begin();

            // 获取channel数据
            Object[] array = takeEventsFromChannel(channel);
            List takeEventList = (List) array[1];
            takedEventSize = (long) takeEventList.size();
            takedEventBytes = Long.parseLong(array[0].toString());
            if ((takedEventSize <= this.minBatchSize) && (takedEventBytes < this.maxBatchBytes)) {
                noDataTimes.getAndIncrement();
            }

            if (takedEventSize <= 0L) {
                transaction.commit();
            } else {
                String sendResult = null;
                String sendStr = objectMapper.writeValueAsString(takeEventList);
                logger.info("sendStr>>>>>>>>>>>>>" + sendStr);
                logger.info("appName" + this.appName);
                Map<Object, Object> headerParams = new HashMap<>();
                ByteArrayOutputStream originalContent = new ByteArrayOutputStream();
                originalContent.write(sendStr.getBytes(StandardCharsets.UTF_8));

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
                originalContent.writeTo(gzipOut);
                gzipOut.finish();
                byte[] payloadByteArray = baos.toByteArray();

                String pathRegex = "[a-zA-Z]/(.*?)\\?";
                String actionRegex = "Action=(.*?)\\&";
                String path = getRegexString(pathRegex, url);
                String action = getRegexString(actionRegex, url);
                String hostUriStr = "http://" + this.host + "/" + path + "?" + this.canonical_querystring;
                logger.info("hostUriStr:" + hostUriStr);
                URI hostUri = URI.create(hostUriStr);
                Map<String, List<String>> parameters = Maps.newHashMap();
                parameters.put("Action", Lists.newArrayList(action));
                parameters.put("Version", Lists.newArrayList("v1"));
                parameters.put("AccountId", Lists.newArrayList("111"));
                parameters.put("tenantId", Lists.newArrayList(tenantId));
                Map<String, String> headers = new HashMap<>();

            }


        } catch (Exception e) {
            logger.error("error while sink:", e);
            transaction.rollback();
            this.counter.incrementRollbackCount();
        } finally {
            this.currentThreads.getAndDecrement();
            transaction.close();
        }
    }

    /**
     * 从channel通道获取event数据
     *
     * @param channel 通道
     * @return
     * @throws UnsupportedEncodingException
     */
    private Object[] takeEventsFromChannel(Channel channel) throws UnsupportedEncodingException {
        Object[] array = new Object[2];
        Event event = null;
        int i = 0;
        long batchBytes = 0L;
        List<String> list = new ArrayList<>();
        // 批量拉取channel数据
        for (i = 0; i < this.batchSize; i++) {
            // 获取一个event
            event = channel.take();
            if (event == null) break;
            String newLine = new String(event.getBody(), StandardCharsets.UTF_8);
            list.add(newLine);
            batchBytes = batchBytes + newLine.getBytes().length;
            // 判断读取的字节码是否超出设置大小
            if (batchBytes > this.maxBatchBytes) {
                logger.info("events bytes beyond threshold , actual bytes: " + batchBytes);
                break;
            }
        }
        array[0] = batchBytes;
        array[1] = list;
        return array;
    }

    /**
     * 判断修改地址超时时间
     *
     * @return
     */
    private boolean isChangeUrlTimeout() {
        return System.currentTimeMillis() - changeUrlStartTime > this.changeUrlTimeout;
    }

    /**
     * 未知IP异常
     */
    private void processUnkownHostException() {
        if (this.ips != null) {
            if (this.isOnlineUrl) {
                url = getBackUrl();
                this.isOnlineUrl = false;
                logger.info("httpsink url change to backup url :" + url);
                changeUrlStartTime = System.currentTimeMillis();
            } else {
                url = this.onlineUrl;
                this.isOnlineUrl = true;
                changeUrlStartTime = 0L;
            }
        } else {
            logger.warn("sink.ips is empty,can't change url to backup url .");
            url = this.onlineUrl;
            this.isOnlineUrl = true;
            changeUrlStartTime = 0L;
        }
    }

    /**
     * 静态加载配置
     */
    static {
        objectMapper.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.getDeserializationConfig().setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
        objectMapper.getSerializationConfig().setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        objectMapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.getSerializationConfig().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);

        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
        schemeRegistry.register(new Scheme("https", 443, PlainSocketFactory.getSocketFactory()));
        cm = new PoolingClientConnectionManager(schemeRegistry);
        initPool();
    }

    /**
     * 正则获取字符串
     *
     * @param regex
     * @param str
     * @return
     */
    private String getRegexString(String regex, String str) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        String result = "";
        while (matcher.find()) {
            result = matcher.group(1);
        }
        return result;
    }
}
