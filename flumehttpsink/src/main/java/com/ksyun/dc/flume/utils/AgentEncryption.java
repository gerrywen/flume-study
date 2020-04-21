package com.ksyun.dc.flume.utils;


/**
 * program: flume-study->AgentEncryption
 * description:
 * author: gerry
 * created: 2020-04-21 10:28
 **/
public class AgentEncryption {
    // 加密
    public static String encode(String sk) {
        byte[] a = new byte[sk.length() + 11];
        byte[] ch = sk.getBytes();

        for (int i = 0; i < 11; i++) {
            a[i] = (byte)(Math.random() * 50 + 40);
        }

        int tmp;
        for (int i = 0; i < sk.length(); i++) {
            tmp = (int) ch[i];
            tmp -= 11;
            a[i+11] = (byte)tmp;
        }

        String str1= new String(a);
        return "aksk2" + str1;
    }

    // 解密
    public static String decode(String secret) {
        String realSk = "";
        for (int i = 16; i < secret.length(); i++) {
            realSk += (char)((int)secret.charAt(i) + 11);
        }
        return realSk;
    }

    public static void main(String[] args) {
        String encode = encode("123456");
        System.out.println(encode);

        String decode = decode(encode);
        System.out.println(decode);

    }
}
