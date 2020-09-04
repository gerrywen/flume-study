package com.myflume.demo.interceptor;

import org.apache.commons.codec.Charsets;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * program: flume-study->OggInterceptor
 * description: OGG数据分发拦截器
 * author: gerry
 * created: 2020-07-29 10:39
 **/
public class OggInterceptor implements Interceptor {

    /**
     * 存放一个事物里面的OGG数据
     */
    List<String> oggList = new ArrayList<>();

    /**
     * 需要发送的数据
     */
    List<Event> oggSendList = new ArrayList<>();

    /**
     * 标记开始位置
     */
    int bCount = 0;
    /**
     * 标记结束位置
     */
    int cCount = 0;
    /**
     * 事物数据递增序号
     */
    int batchCount = 0;

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        System.out.println("----------intercept(Event event)方法执行，处理单个event");
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        System.out.println("OGG数据进来方法了吗？");
        // 有正则的情况
        for (int h = 0; h < list.size(); h++) {
            Event interceptedEvent  = null;
            interceptedEvent = intercept((Event)list.get(h));
            // 单个的body
            String singleBody = new String(interceptedEvent.getBody(), Charsets.UTF_8);
            System.out.println("单个的body数据：" + singleBody);

            if (singleBody.startsWith("B")) {
                bCount += 1;
            } else if (singleBody.startsWith("C")) {
                cCount += 1;
            } else {
                batchCount += 1;
                String tempStr = String.valueOf(batchCount) + "|" + singleBody;
                oggList.add(tempStr);
            }

            if (bCount == 1 && cCount == 1) {
                Long idWorker = getIdWorker();
                String uuid = String.valueOf(idWorker);
                // 这里是正确的数据，处理list转换json
                bCount = cCount = 0;
                // 初始化map
                Map<String, Object> tmpMap = new HashMap<>();
                Map<String, String> dataMap = new HashMap<>();
                for (int i = 0; i < oggList.size(); i = i + 1) {
                    tmpMap.put("uuid", uuid);
                    tmpMap.put("size", String.valueOf(oggList.size()));
                    // 分割字符
                    String[] splitOgg = oggList.get(i).split("\\|");
                    String[] keys = {"sort", "type", "opt_type", "db_table", "opt_time"};
                    for (int j = 0; j < splitOgg.length; j = j + 1) {
                        if (j < keys.length) {
                            tmpMap.put(keys[j], splitOgg[j]);
                        }
                    }

                    for (int j = 5; j < splitOgg.length; j = j + 2) {
                        String key = splitOgg[j];
                        String value = "";
                        if (j + 1 < splitOgg.length) {
                            value = splitOgg[j + 1];
                        }
                        dataMap.put(key, value);
                    }
                    tmpMap.put("data", dataMap);
                    // 添加到oggList
                    // 处理完的数据重新赋值body
                    System.out.println("处理完的数据重新赋值body : " + tmpMap.toString());
                    interceptedEvent.setBody(tmpMap.toString().getBytes());
                    oggSendList.add(interceptedEvent);
                    // 清除临时map
                    tmpMap.clear();
                }
                oggList.clear();
                oggSendList.clear();
            }else if (bCount == 0 && cCount == 1) {
                cCount = 0;
                // 说明有结尾没开头，这条数据是有问题的
                System.out.println("数据有误：" + oggList.toString());
                oggList.clear();
            } else if (bCount > 1 || cCount > 1) {
                // 说明累加超过了，也是数据有问题
                bCount = cCount = 0;
                System.out.println("数据累加过多错误：" + oggList.toString());
                oggList.clear();
            }
        }
        System.out.println("oggSendList :" + oggSendList.toString());
        return oggSendList;
    }

    @Override
    public void close() {

    }

    public static Long getIdWorker() {
        IdWorker idWorker= new IdWorker(1,1);
        return idWorker.nextId();
    }
}
