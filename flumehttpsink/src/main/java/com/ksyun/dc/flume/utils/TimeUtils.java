package com.ksyun.dc.flume.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.Vector;

/**
 * program: flume-study->TimeUtils
 * description: 时间工具
 * author: gerry
 * created: 2020-04-20 14:42
 **/
public class TimeUtils {

    /**
     * 天
     *
     * @param d
     * @param amount
     * @return
     */
    public static Date calculateByDate(Date d, int amount) {
        return calculate(d, GregorianCalendar.DATE, amount);
    }

    /**
     * 分钟
     *
     * @param d
     * @param amount
     * @return
     */
    public static Date calculateByMinute(Date d, int amount) {
        return calculate(d, GregorianCalendar.MINUTE, amount);
    }

    /**
     * 年
     *
     * @param d
     * @param amount
     * @return
     */
    public static Date calculateByYear(Date d, int amount) {
        return calculate(d, GregorianCalendar.YEAR, amount);
    }


    public static Date calculate(Date d, int field, int amount) {
        if (d == null) {
            return null;
        }
        GregorianCalendar g = new GregorianCalendar();
        g.setGregorianChange(d);
        g.add(field, amount);
        return g.getTime();
    }

    public static String date2String(String formater, Date date) {
        if (formater == null || "".equals(formater)) {
            return null;
        }
        if (date == null) {
            return null;
        }
        return (new SimpleDateFormat(formater)).format(date);
    }

    public static String date2String(String formater) {
        return date2String(formater, new Date());
    }

    public static String date2StringByDate(String formater, Date date) {
        return date2String(formater, date);
    }

    public static int dayOfWeek() {
        GregorianCalendar g = new GregorianCalendar();
        int ret = g.get(Calendar.DAY_OF_WEEK);
        g = null;
        return ret;
    }

    public static String[] fetchAllTimeZoneIds() {
        Vector v = new Vector<>();
        String[] ids = TimeZone.getAvailableIDs();
        for (int i = 0; i < ids.length; i++) {
            v.add(ids[i]);
        }
        Collections.sort(v, String.CASE_INSENSITIVE_ORDER);
        v.copyInto(ids);
        v = null;
        return ids;
    }

    public static String getWordTime(Date date) {
        String[] ids = fetchAllTimeZoneIds();
        String nowDateTime = date2StringByDate("yyyy-MM-dd HH:mm:ss", date);
        return string2TimezoneDefault(nowDateTime, "Etc/GMT");
    }

    public static String string2TimeZone(String srcFormater, String srcDateTime, String dstFormater, String dstTimeZoneId) {
        if (srcFormater == null || "".equals(srcFormater)) return null;
        if (srcDateTime == null || "".equals(srcDateTime)) return null;
        if (dstFormater == null || "".equals(dstFormater)) return null;
        if (dstTimeZoneId == null || "".equals(dstTimeZoneId)) return null;

        SimpleDateFormat sdf = new SimpleDateFormat(srcFormater);
        try {
            int diffTime = getDiffTimeZoneRawOffset(dstTimeZoneId);
            Date d = sdf.parse(srcDateTime);
            long nowTime = d.getTime();
            long newNowTime = nowTime - diffTime;
            d = new Date(newNowTime);
            return date2String(dstFormater, d);
        } catch (ParseException e) {
            return null;
        }
    }


    public static String string2TimezoneDefault(String srcDateTime, String dstTimeZoneId) {
        return string2TimeZone("yyyy-MM-dd HH:mm:ss", srcDateTime, "yyyyMMdd'T'HHmmss'Z'", dstTimeZoneId);
    }

    private static int getDiffTimeZoneRawOffset(String timeZoneId) {
        return TimeZone.getDefault().getRawOffset() - TimeZone.getTimeZone(timeZoneId).getRawOffset();
    }

    private static int getTimeZoneRawOffset(String timeZoneId) {
        return TimeZone.getTimeZone(timeZoneId).getRawOffset();
    }

    private static int getDefaultTimeZoneRawOffset() {
        return TimeZone.getDefault().getRawOffset();
    }

    public static void main(String[] args) {
        String[] ids = fetchAllTimeZoneIds();
        String nowDateTime = date2String("yyyy-MM-dd HH:mm:ss");
        System.out.println("The time Asia/Shanghai is " + nowDateTime);
        for(int i = 0; i < ids.length ; i++) {
            System.out.println(" * " + ids[i] + "=" + string2TimezoneDefault(nowDateTime, ids[i]));

        }

        System.out.println("TimeZone.getDefault().getID() = " + TimeZone.getDefault().getID());
        System.out.println(getWordTime(new Date()));
    }

}
