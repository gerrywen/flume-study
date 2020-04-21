package com.ksyun.dc.flume.utils;

import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.internal.SignerConstants;
import com.amazonaws.http.HttpMethodName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * program: flume-study->AwsSignerV4Util
 * description:
 * author: gerry
 * created: 2020-04-21 10:37
 **/
public class AwsSignerV4Util {
    // 日志打印
    private static final Logger logger = LoggerFactory.getLogger(AwsSignerV4Util.class);

    public static List<Header> getAuthHeaderForGet(URI uri, Map<String, List<String>> parameters,
                                                   Map<String, String> headers, String service, String region, String ak, String sk) throws Exception {
        Map<String, List<String>> _parameters = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : parameters.entrySet()) {
            String key = entry.getKey();
            List<String> list = entry.getValue();
            if (list != null) {
                _parameters.put(key, list);
            }
        }

        logger.info("The Request parameter value {} : ", _parameters);

        Request<String> request = new DefaultRequest<String>(service);
        request.setHttpMethod(HttpMethodName.GET);

        String scheme = uri.getScheme();
        String beforeAuthority = scheme == null ? "" : scheme + "://";
        String authority = uri.getAuthority();
        String path = uri.getPath();
        request.setEndpoint(URI.create(beforeAuthority + authority));
        request.setResourcePath(path);

        headers = headers == null ? new HashMap<String, String>() : headers;

        if (headers.containsKey("X-Amz-Date")) {
            headers.remove("X-Amz-Date");
        }
        if (headers.containsKey("X-KSC-SERVICE")) {
            headers.remove("X-KSC-SERVICE");
        }
        if (headers.containsKey("X-KSC-REGION")) {
            headers.remove("X-KSC-REGION");
        }
        if (headers.containsKey("Authorization")) {
            headers.remove("Authorization");
        }

        request.setHeaders(headers);
        request.setParameters(_parameters);
        request.setContent(null);

        AWSCredentials credentials = new BasicAWSCredentials(ak, sk);
        AWS4Signer aws4Signer = new AWS4Signer();
        aws4Signer.setServiceName(service);
        aws4Signer.setRegionName(region);
        aws4Signer.sign(request, credentials);

        return buildSignHeader(request, service, region, headers);

    }


    public static List<Header> getAuthHeaderForPost(URI uri, String body, Map<String, String> headers,
                                                    String service, String region, String ak, String sk) throws Exception {
        Request<String> request = new DefaultRequest<String>(service);
        request.setHttpMethod(HttpMethodName.POST);

        String scheme = uri.getScheme();
        String beforeAuthority = scheme == null ? "" : scheme + "://";
        String authority = uri.getAuthority();
        String path = uri.getPath();
        request.setEndpoint(URI.create(beforeAuthority + authority));
        request.setResourcePath(path);

        headers = headers == null ? new HashMap<String, String>() : headers;

        if (headers.containsKey("X-Amz-Date")) {
            headers.remove("X-Amz-Date");
        }
        if (headers.containsKey("X-KSC-SERVICE")) {
            headers.remove("X-KSC-SERVICE");
        }
        if (headers.containsKey("X-KSC-REGION")) {
            headers.remove("X-KSC-REGION");
        }
        if (headers.containsKey("Authorization")) {
            headers.remove("Authorization");
        }

        request.setHeaders(headers);

        String contentType = headers.get("Content-type");
        logger.info("Request contentType is {}:", contentType);

        if (body == null) {
            request.setContent(null);
        } else {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
            request.setContent(inputStream);
        }


        AWSCredentials credentials = new BasicAWSCredentials(ak, sk);
        AWS4Signer aws4Signer = new AWS4Signer();
        aws4Signer.setServiceName(service);
        aws4Signer.setRegionName(region);
        aws4Signer.sign(request, credentials);

        return buildSignHeader(request, service, region, headers);

    }


    public static List<Header> getAuthHeaderForDelete(URI uri, Map<String, List<String>> parameters,
                                                      Map<String, String> headers, String service, String region, String ak, String sk) throws Exception {
        Map<String, List<String>> _parameters = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : parameters.entrySet()) {
            String key = entry.getKey();
            List<String> list = entry.getValue();
            if (list != null) {
                _parameters.put(key, list);
            }
        }

        logger.info("The Request parameter value {} : ", _parameters);

        Request<String> request = new DefaultRequest<String>(service);
        request.setHttpMethod(HttpMethodName.DELETE);

        String scheme = uri.getScheme();
        String beforeAuthority = scheme == null ? "" : scheme + "://";
        String authority = uri.getAuthority();
        String path = uri.getPath();
        request.setEndpoint(URI.create(beforeAuthority + authority));
        request.setResourcePath(path);

        headers = headers == null ? new HashMap<String, String>() : headers;

        if (headers.containsKey("X-Amz-Date")) {
            headers.remove("X-Amz-Date");
        }
        if (headers.containsKey("X-KSC-SERVICE")) {
            headers.remove("X-KSC-SERVICE");
        }
        if (headers.containsKey("X-KSC-REGION")) {
            headers.remove("X-KSC-REGION");
        }
        if (headers.containsKey("Authorization")) {
            headers.remove("Authorization");
        }

        request.setHeaders(headers);
        request.setParameters(_parameters);
        request.setContent(null);

        AWSCredentials credentials = new BasicAWSCredentials(ak, sk);
        AWS4Signer aws4Signer = new AWS4Signer();
        aws4Signer.setServiceName(service);
        aws4Signer.setRegionName(region);
        aws4Signer.sign(request, credentials);

        return buildSignHeader(request, service, region, headers);

    }

    public static List<Header> getAuthHeaderForPut(URI uri, String body, Map<String, String> headers,
                                                   String service, String region, String ak, String sk) throws Exception {
        Request<String> request = new DefaultRequest<String>(service);
        request.setHttpMethod(HttpMethodName.PUT);

        String scheme = uri.getScheme();
        String beforeAuthority = scheme == null ? "" : scheme + "://";
        String authority = uri.getAuthority();
        String path = uri.getPath();
        request.setEndpoint(URI.create(beforeAuthority + authority));
        request.setResourcePath(path);

        headers = headers == null ? new HashMap<String, String>() : headers;

        if (headers.containsKey("X-Amz-Date")) {
            headers.remove("X-Amz-Date");
        }
        if (headers.containsKey("X-KSC-SERVICE")) {
            headers.remove("X-KSC-SERVICE");
        }
        if (headers.containsKey("X-KSC-REGION")) {
            headers.remove("X-KSC-REGION");
        }
        if (headers.containsKey("Authorization")) {
            headers.remove("Authorization");
        }

        request.setHeaders(headers);

//        String contentType = headers.get("Content-type");
//        logger.info("Request contentType is {}:" , contentType);

        if (body == null) {
            request.setContent(null);
        } else {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
            request.setContent(inputStream);
        }


        AWSCredentials credentials = new BasicAWSCredentials(ak, sk);
        AWS4Signer aws4Signer = new AWS4Signer();
        aws4Signer.setServiceName(service);
        aws4Signer.setRegionName(region);
        aws4Signer.sign(request, credentials);

        return buildSignHeader(request, service, region, headers);

    }

    private static List<Header> buildSignHeader(Request<String> request, String service,
                                                String region, Map<String, String> headers) {
        List<Header> headers2 = new ArrayList<Header>();
        headers2.add(new Header("Authorization", request.getHeaders().get(SignerConstants.AUTHORIZATION)));
        headers2.add(new Header("X-Amz-Date", request.getHeaders().get(SignerConstants.X_AMZ_DATE)));
        headers2.add(new Header("X-KSC-SERVICE", service));
        headers2.add(new Header("X-KSC-REGION", region));
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            headers2.add(new Header(entry.getKey(), entry.getValue()));
        }
        logger.info("The list of header value : {}", headers2.toString());

        return headers2;
    }
}
