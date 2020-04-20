package com.ksyun.dc.flume.utils;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

/**
 * program: flume-study->HttpClientFactory
 * description:
 * author: gerry
 * created: 2020-04-20 11:19
 **/
public class HttpClientFactory {

    // 日志打印
    private static final Logger logger = LoggerFactory.getLogger(HttpClientFactory.class);

    private static CloseableHttpClient client;

    public static CloseableHttpClient getHttpsClient() {
        if (client != null) {
            return client;
        }
        try {
            SSLContext sslContext = SSLContexts.custom().useSSL().build();
            sslContext.init(null, new X509TrustManager[]{new HttpsTrustManager()}, new SecureRandom());
            SSLConnectionSocketFactory factory = new SSLConnectionSocketFactory(sslContext, new String[]{"TLSv1"}, null,
                    SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
            client = HttpClients.custom().setSSLSocketFactory(factory).build();

        } catch (NoSuchAlgorithmException e) {
            logger.error("HttpClientFactory NoSuchAlgorithmException :" + e.toString());
            e.printStackTrace();
        } catch (KeyManagementException e) {
            logger.error("HttpClientFactory KeyManagementException :" + e.toString());
            e.printStackTrace();
        }

        return client;
    }

    public static void releaseInstance() {
        client = null;
    }

}
