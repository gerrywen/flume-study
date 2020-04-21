package com.ksyun.dc.flume.utils;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * program: flume-study->Base64Util
 * description:
 * author: gerry
 * created: 2020-04-21 15:15
 **/
public class Base64Util {
    private static final Logger logger = LoggerFactory.getLogger(Base64Util.class);

    private static final String charset = "UTF-8";

    public static String decode(String data) {
        try {
            if (null == data) {
                return null;
            }
            return new String(Base64.decodeBase64(data.getBytes(charset)), charset);
        } catch (UnsupportedEncodingException e) {
            logger.error(String.format("字符串 ：%s,解密异常", data));
            e.printStackTrace();
        }
        return null;
    }

    public static String encode(String data) {
        try {
            if (null == data) {
                return null;
            }
            return new String(Base64.encodeBase64(data.getBytes(charset)), charset);
        } catch (UnsupportedEncodingException e) {
            logger.error(String.format("字符串 ：%s,加密异常", data));
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        String str = "123XXX456";
        String encode = encode(str);
        System.out.println(encode);

        String decode = decode(encode);
        System.out.println(decode);
    }
}
