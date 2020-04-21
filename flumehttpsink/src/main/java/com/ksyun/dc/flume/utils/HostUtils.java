package com.ksyun.dc.flume.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * program: flume-study->HostUtils
 * description:
 * author: gerry
 * created: 2020-04-21 15:27
 **/
public class HostUtils {
    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    public static String getLocalIP() {
        String ip = "";
        try {
            Enumeration<?> e1 = NetworkInterface.getNetworkInterfaces();
            while (e1.hasMoreElements()) {
                NetworkInterface ni = (NetworkInterface) e1.nextElement();
                Enumeration<?> e2 = ni.getInetAddresses();
                while (e2.hasMoreElements()) {
                    InetAddress ia = (InetAddress) e2.nextElement();

                    if (ia instanceof Inet6Address) {
                        continue;
                    }
                    if (ia.isLinkLocalAddress()) {
                        continue;
                    }
                    if (ni.getName().equals("eth0") || ni.getName().equals("bond1")) {
                        return ia.getHostAddress();
                    } else {
                        ip = ia.getHostAddress();
                    }
                }
            }

        } catch (Exception e) {
            logger.error("get ip error." + e);
            e.printStackTrace();
        }
        return ip;
    }

    public static String getHostName() {
        try {
            return (InetAddress.getLocalHost()).getHostName();
        } catch (UnknownHostException uhe) {
            String host = uhe.getMessage();
            if (host != null) {
                int colon = host.indexOf(":");
                if (colon > 0) {
                    return host.substring(0, colon);
                }

            }else {
                host = System.getenv().get("HOSTNAME");
                if (host != null) {
                    return host;
                }
            }
            return "UnknownHost";
        } catch (Exception e) {
            logger.error("get host error." + e);
            return "-";
        }
    }

    public static void main(String[] args) {
        System.out.println(getLocalIP());
        System.out.println(getHostName());
    }

}
