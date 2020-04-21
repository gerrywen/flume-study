package com.ksyun.dc.flume.utils;

import com.amazonaws.auth.SigningAlgorithm;
import com.google.common.primitives.Chars;
import net.sf.json.JSONObject;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static com.amazonaws.auth.internal.SignerConstants.AWS4_TERMINATOR;

/**
 * program: flume-study->SignatureUtils
 * description:
 * author: gerry
 * created: 2020-04-21 15:40
 **/
public class SignatureUtils {

    private static final char[] HEX_CHAR = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'f'};

    private static byte[] HmacSHA256(String data, byte[] key)
            throws Exception {
        String algorithm = "HmacSHA256";
        Mac mac = Mac.getInstance(algorithm);
        mac.init(new SecretKeySpec(key, algorithm));
        return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] getSignatureKey(String key, String dateStamp, String regionName, String serviceName)
            throws Exception {
        byte[] kSecret = ("AWS4" + key).getBytes(StandardCharsets.UTF_8);
        byte[] kDate = HmacSHA256(dateStamp, kSecret);
        byte[] kRegion = HmacSHA256(regionName, kDate);
        byte[] kService = HmacSHA256(serviceName, kRegion);
        return HmacSHA256("aws4_request", kService);
    }

    private static String bytesToHexFun2(byte[] bytes) {
        char[] buf = new char[bytes.length * 2];
        int index = 0;
        for (byte b : bytes) {
            buf[index++] = HEX_CHAR[b >>> 4 & 0xf];
            buf[index++] = HEX_CHAR[b & 0Xf];
        }
        return new String(buf);
    }

    public static String getSHA256StrJava(byte[] bytes) {
        MessageDigest messageDigest;
        String encodeStr = "";
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(bytes);
            encodeStr = byte2Hex(messageDigest.digest());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return encodeStr;
    }

    public static String getSHA256StrJava(String str) {
        MessageDigest messageDigest;
        String encodeStr = "";
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(str.getBytes(StandardCharsets.UTF_8));
            encodeStr = byte2Hex(messageDigest.digest());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return encodeStr;
    }


    public static String byte2Hex(byte[] bytes) {
        StringBuffer stringBuffer = new StringBuffer();
        String temp = null;
        for (int i = 0; i < bytes.length; i++) {
            temp = Integer.toHexString(bytes[i] & 0xFF);
            if (temp.length() == 1) {
                // 1得到一位的进行补0操作
                stringBuffer.append("0");
            }
            stringBuffer.append(temp);
        }
        return stringBuffer.toString();
    }


    public static String getSignature(JSONObject params, byte[] payloadByteArray) throws Exception {
        String secret_key = params.getString("secret_key");
        String credentialData = params.getString("credentialData");
        String region = params.getString("region");
        String service = params.getString("service");
        String amzdate = params.getString("amzdate");
        String method = params.getString("method");
        String canonical_uri = params.getString("canonical_uri");
        String canonical_querystring = params.getString("canonical_querystring");
        String host = params.getString("host");
        String signedHeaders = params.getString("signedHeaders");
        String content_type = params.getString("content-type");
        String algorithm = params.getString("algorithm");
        String payload = params.getString("payload");
        String comresssEanble = params.getString("comresssEanble");

        String payload_hash = "";
        if ("true".equals(comresssEanble)) {
            payload_hash = SignatureUtils.getSHA256StrJava(payloadByteArray);
        } else {
            payload_hash = SignatureUtils.getSHA256StrJava(payload);
        }

        String canonical_headers = "content-type:" + content_type + "\n" + "host:" + host + "\n" + "x-amz-date:" + amzdate + "\n";
        String credential_scope = credentialData + "/" + region + "/" + service + "/" + "aws4_request";
        String canonical_request = method + '\n' + canonical_uri + '\n' + canonical_querystring + '\n'
                + canonical_headers + '\n' + signedHeaders + '\n' + payload_hash;
        byte[] signing_key = getSignatureKey(secret_key, credentialData, region, service);
        String string_to_sign = algorithm + '\n' + amzdate + '\n' + credential_scope + '\n' + SignatureUtils.getSHA256StrJava(canonical_request);

        byte[] signatureByte = HmacSHA256(string_to_sign, signing_key);
        return bytesToHexFun2(signatureByte);
    }

    public static String getAuthorization(JSONObject params, byte[] payloadByteArray) throws Exception {
        String algorithm = params.getString("algorithm");
        String accessKey = params.getString("accessKey");
        String credentialData = params.getString("credentialData");
        String region = params.getString("region");
        String service = params.getString("service");
        String signedHeaders = params.getString("signedHeaders");

        String signature = SignatureUtils.getSignature(params, payloadByteArray);
        String credential = accessKey + "/" + credentialData + "/" + region + "/" + service + "/aws4_request";
        return algorithm + " " + "Credential=" + credential + ", " + "SignedHeaders=" + signedHeaders
                + ", " + "Signature=" + signature;
    }

}
