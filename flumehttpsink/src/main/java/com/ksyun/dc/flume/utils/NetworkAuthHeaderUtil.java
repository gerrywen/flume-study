package com.ksyun.dc.flume.utils;

import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.http.HttpMethodName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * program: flume-study->NetworkAuthHeaderUtil
 * description: 网络请求头部权限控制
 * author: gerry
 * created: 2020-04-20 16:40
 **/
public class NetworkAuthHeaderUtil {

    private static final Logger logger = LoggerFactory.getLogger(NetworkAuthHeaderUtil.class);

    public static List<Header> getAuthHeaderForJson(URI uri, Map<String, List<String>> parameters, String body, HttpMethod method,
                                                    Map<String, String> headers, String service, String region, String ak, String sk) throws Exception {

        Map<String, List<String>> _parameters = getParameters(parameters);
        Request<String> request1 = new DefaultRequest<String>(service);
        String beforeAuthority = null;
        request1.setHttpMethod(transfer(method));
        String scheme = uri.getScheme();
        beforeAuthority = scheme == null ? "" : scheme + "://";


        String authority1 = uri.getAuthority();
        String path = uri.getPath();
        request1.setEndpoint(URI.create(beforeAuthority + authority1));
        request1.setResourcePath(path);

        Object headers1 = headers == null ? new HashMap<String, String>() : headers;

        if (((Map) headers1).containsKey("X-Amz-Date")) {
            ((Map) headers1).remove("X-Amz-Date");
        }
        if (((Map) headers1).containsKey("X-KSC-SERVICE")) {
            ((Map) headers1).remove("X-KSC-SERVICE");
        }
        if (((Map) headers1).containsKey("X-KSC-REGION")) {
            ((Map) headers1).remove("X-KSC-REGION");
        }
        if (((Map) headers1).containsKey("Authorization")) {
            ((Map) headers1).remove("Authorization");
        }

        request1.setHeaders(((Map) headers1));
        request1.setParameters(_parameters);


        if (body == null) {
            request1.setContent((InputStream) null);
        } else {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
            request1.setContent(inputStream);
        }


        BasicAWSCredentials credentials = new BasicAWSCredentials(ak, sk);
        AWS4Signer aws4Signer = new AWS4Signer();
        aws4Signer.setServiceName(service);
        aws4Signer.setRegionName(region);
        aws4Signer.sign(request1, credentials);

        List<Header> headers2 = new ArrayList<Header>();
        headers2.add(new Header("Authorization", (String) request1.getHeaders().get("Authorization")));
        headers2.add(new Header("X-Amz-Date", (String) request1.getHeaders().get("X-Amz-Date")));
        headers2.add(new Header("X-KSC-SERVICE", service));
        headers2.add(new Header("X-KSC-REGION", region));
        Iterator var16 = ((Map) headers1).entrySet().iterator();

        while (var16.hasNext()) {
            Map.Entry entry = (Map.Entry) var16.next();
            headers2.add(new Header((String) entry.getKey(), (String) entry.getValue()));
        }
        return headers2;
    }


    public static List<Header> getAuthHeaderForJson(URI uri, Map<String, List<String>> parameters, byte[] body, HttpMethod method,
                                                    Map<String, String> headers, String service, String region, String ak, String sk) throws Exception {

        Map<String, List<String>> _parameters = getParameters(parameters);
        Request<String> request1 = new DefaultRequest<String>(service);
        String beforeAuthority = null;
        request1.setHttpMethod(transfer(method));
        String scheme = uri.getScheme();
        beforeAuthority = scheme == null ? "" : scheme + "://";


        String authority1 = uri.getAuthority();
        String path = uri.getPath();
        request1.setEndpoint(URI.create(beforeAuthority + authority1));
        request1.setResourcePath(path);

        Object headers1 = headers == null ? new HashMap<String, String>() : headers;

        if (((Map) headers1).containsKey("X-Amz-Date")) {
            ((Map) headers1).remove("X-Amz-Date");
        }
        if (((Map) headers1).containsKey("X-KSC-SERVICE")) {
            ((Map) headers1).remove("X-KSC-SERVICE");
        }
        if (((Map) headers1).containsKey("X-KSC-REGION")) {
            ((Map) headers1).remove("X-KSC-REGION");
        }
        if (((Map) headers1).containsKey("Authorization")) {
            ((Map) headers1).remove("Authorization");
        }

        request1.setHeaders(((Map) headers1));
        request1.setParameters(_parameters);


        if (body == null) {
            request1.setContent((InputStream) null);
        } else {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(body);
            request1.setContent(inputStream);
        }


        BasicAWSCredentials credentials = new BasicAWSCredentials(ak, sk);
        AWS4Signer aws4Signer = new AWS4Signer();
        aws4Signer.setServiceName(service);
        aws4Signer.setRegionName(region);
        aws4Signer.sign(request1, credentials);

        List<Header> headers2 = new ArrayList<Header>();
        headers2.add(new Header("Authorization", (String) request1.getHeaders().get("Authorization")));
        headers2.add(new Header("X-Amz-Date", (String) request1.getHeaders().get("X-Amz-Date")));
        headers2.add(new Header("X-KSC-SERVICE", service));
        headers2.add(new Header("X-KSC-REGION", region));
        Iterator var16 = ((Map) headers1).entrySet().iterator();

        while (var16.hasNext()) {
            Map.Entry entry = (Map.Entry) var16.next();
            headers2.add(new Header((String) entry.getKey(), (String) entry.getValue()));
        }
        return headers2;
    }


    private static Map<String, List<String>> getParameters(Map<String, List<String>> parameters) {
        Map<String, List<String>> _parameters = new HashMap<>();
        Iterator request = parameters.entrySet().iterator();

        String beforeAuthority;
        while (request.hasNext()) {
            Map.Entry scheme = (Map.Entry) request.next();
            beforeAuthority = (String) scheme.getKey();
            List<String> authority = (List) scheme.getValue();
            if (authority != null) {
                _parameters.put(beforeAuthority, authority);
            }
        }
        return _parameters;
    }


    private static HttpMethodName transfer(HttpMethod method) {
        for (HttpMethodName httpMethodName : HttpMethodName.values()) {
            if (httpMethodName.name().equals(method.name())) {
                return httpMethodName;
            }
        }
        logger.error("没有找到对应HTTP请求类型[" + method.name() + "]");

        return null;
    }

}
