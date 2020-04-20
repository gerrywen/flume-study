package com.ksyun.dc.flume.utils;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import java.security.Key;
import java.security.SecureRandom;

/**
 * program: flume-study->SecurityUtil
 * description: 加密
 * author: gerry
 * created: 2020-04-20 14:04
 **/
public class SecurityUtil {
    public static String DES = "AES";// optional value AES/DES/DESede

    public static String CIPHER_ALGORITHM = "AES";

    public static Key getKey(String strKey) {
        try {
            if (strKey == null) {
                strKey = "";
            }
            KeyGenerator _generator = KeyGenerator.getInstance("AES");
            SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
            secureRandom.setSeed(strKey.getBytes());
            _generator.init(128, secureRandom);
            return _generator.generateKey();
        }catch (Exception e) {
            throw new RuntimeException("初始化秘钥出现异常");
        }
    }

    public static String encrypt(String data, String key) throws Exception {
        SecureRandom sr = new SecureRandom();
        Key secureKey = getKey(key);
        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, secureKey, sr);
        byte[] bt = cipher.doFinal(data.getBytes());
        return new BASE64Encoder().encode(bt);
    }

    public static String decrypt(String data, String key) throws Exception {
        SecureRandom sr = new SecureRandom();
        Key secureKey = getKey(key);
        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, secureKey, sr);
        byte[] res = new BASE64Decoder().decodeBuffer(data);
        res = cipher.doFinal(res);
        return new String(res);
    }

    public static void main(String[] args) throws Exception {
        String message = "1";
        String key = "1";
        String encrypt = encrypt(message, key);
        System.out.println("encrypted message is below : ");
        System.out.println(encrypt);


        String decrypt = decrypt(encrypt, key);
        System.out.println("decrypted message is below : ");
        System.out.println(decrypt);
    }

}
