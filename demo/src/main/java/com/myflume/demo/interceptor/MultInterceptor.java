package com.myflume.demo.interceptor;

import com.google.common.collect.Lists;
import org.apache.commons.codec.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * program: flume-study->MultInterceptor
 * description: Flume 自定义拦截器 多行读取日志+截断
 * author: gerry
 * created: 2020-07-27 14:42
 *
 * 使用说明：在flume启动配置文件增加以下内容：
 * #匹配时间并转换为时间戳到header中
 * a1.sources.tail.interceptors.i2.type=org.apache.flume.custom.MultInterceptor$Builder
 * #正则表达式，按需求定
 * a1.sources.tail.interceptors.i2.regex=(((?!0000)[0-9]{4}-((0[1-9]|1[0-2])-(0[1-9]|1[0-9]|2[0-8])|(0[13-9]|1[0-2])-(29|30)|(0[13578]|1[02])-31)|([0-9]{2}(0[48]|[2468][048]|[13579][26])|(0[48]|[2468][048]|[13579][26])00)-02-29))
 * #开启日志长度截取标志，默认true，开启
 * a1.sources.tail.interceptors.i2.cutFlag = true
 * #最大截取字符串长度,整数,尽量控制在2M以内，单位：kb，1M=1024
 * a1.sources.tail.interceptors.i2.cutMax = 2048
 * #单个截取字符串长度，整数，尽量控制在1.5M以内，单位：kb,1M=1024
 * a1.sources.tail.interceptors.i2.singleCut=1024
 * a1.sources.tail.interceptors.i2.serializers=se1
 * a1.sources.tail.interceptors.i2.serializers.se1.type=org.apache.flume.interceptor.RegexExtractorInterceptorMillisSerializer
 * a1.sources.tail.interceptors.i2.serializers.se1.name=timestamp
 * a1.sources.tail.interceptors.i2.serializers.se1.pattern=yyyy-MM-dd
 *
 **/
public class MultInterceptor implements Interceptor {

    // 过滤正则
    private static Pattern regex = null;
    // 截取标志
    private static Boolean cutFlag = true;
    // 总截取最大长度
    private static Integer cutMax = null;
    // 单个截取最大长度
    private static Integer singleCut = null;
    // 最后一个事件流
    private static List<Event> lastList = Lists.newArrayList();

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
        System.out.println("进来方法了吗？");
        // 处理结果 event list
        List<Event> intercepted = null;
        int addnum = 0;// 记录上一个正确匹配的event在队列中的位置,以便下一event有和它连接的需要
        if (lastList != null && lastList.size() >0) {
            // 初始化
            int initCapacity = list.size() + lastList.size();
            intercepted = Lists.newArrayListWithCapacity(initCapacity);
            // 添加
            intercepted.addAll(lastList);
            // 清空
            lastList = Lists.newArrayList();
        } else {
            intercepted = Lists.newArrayListWithCapacity(list.size());
        }
        // 有正则的情况
        for (int i = 0; i < list.size(); i++) {
            Event interceptedEvent  = null;
            Matcher matcher = regex.matcher(new String(list.get(i).getBody(), Charsets.UTF_8));
            if (matcher.find()) {
                interceptedEvent = intercept((Event)list.get(i));
                // 单个的body
                String singleBody = new String(interceptedEvent.getBody(), Charsets.UTF_8);
                int singleBodyLen = singleBody.length();
                System.out.println("正则匹配-原始body---------:" + singleBody);
                if (cutFlag){
                    // 处理最大截取数边界条件--一定要重新一个变量接收
                    int lsSingleCut = singleCut > singleBodyLen ? singleBodyLen : singleCut;
                    // 截取字符串--新变量
                    String singleCutBody = new String(singleBody.substring(0, lsSingleCut));
                    System.out.println("单个截取-截取后body=============:" + singleCutBody);
                    // 重新赋值body
                    interceptedEvent.setBody(singleCutBody.getBytes());
                }
                intercepted.add(interceptedEvent);
                addnum = addnum +1;
                System.out.println("matcher.find() 下的：addnum：" + addnum);
            } else {
                if (intercepted.size() == 0){
                    // 表示本次没有匹配上
                    continue;
                }
                addnum = addnum >= intercepted.size() ? intercepted.size() - 1 : addnum;
                String body = new String(intercepted.get(addnum).getBody(), Charsets.UTF_8) + "\n" + new String(list.get(i).getBody(), Charsets.UTF_8);
                System.out.println("总截取-原始body---------:" + body);
                int bodyLen = body.length();
                // 截取body-新变量
                String cutBody = body;
                if (cutFlag) {
                    // 处理最大截取数边界条件--新变量
                    int lsCutMax = cutMax > bodyLen ? bodyLen : cutMax;
                    // 截取字符串
                    cutBody = new String(body.substring(0, lsCutMax));
                    System.out.println("-处理截取-截取后body=============: " + body);
                }
                intercepted.get(addnum).setBody(cutBody.getBytes());
            }
        }

        // 最后一个保存在静态变量，等待下一批次
        if (intercepted != null && intercepted.size() > 0) {
            int lastIndex = intercepted.size() -1;
            lastList.add(intercepted.get(lastIndex));
            // 移除最后一个索引
            intercepted.remove(lastIndex);
        }
        return intercepted;
    }

    @Override
    public void close() {
        System.out.println("----------自定义拦截器close方法执行");
    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            System.out.println("----------build方法执行");
            return new MultInterceptor();
        }

        @Override
        public void configure(Context context) {
            String regexStr = context.getString("regex", null);
            cutFlag = context.getBoolean("cutFlag", true);
            cutMax = context.getInteger("cutMax", 0);
            singleCut = context.getInteger("singleCut", 0);
            System.out.println("参数regexStr：" + regexStr + ",参数cutMax： " + cutMax + ",cutFlag: " + cutFlag + " ,singleCut: " + singleCut);
            // 由于外面传过来的单位是kb，所以这边需要乘以1024
            cutMax = cutMax * 1024;
            System.out.println("总截取最大值：" + cutMax);
            singleCut = singleCut * 1024;
            System.out.println("单个截取最大值：" + singleCut);
            if (null != regexStr) {
                // 转换正则
                regex = Pattern.compile(regexStr);
            }
        }
    }

}
