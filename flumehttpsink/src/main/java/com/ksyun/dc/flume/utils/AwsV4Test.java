package com.ksyun.dc.flume.utils;

import org.springframework.http.HttpMethod;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ksyun.dc.flume.sink.DataSink;
import net.sf.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

/**
 * program: flume-study->AwsV4Test
 * description:
 * author: gerry
 * created: 2020-04-21 11:29
 **/
public class AwsV4Test {
    public static void main(String[] args) throws Exception {
        String principalkey = "AKLTm7eDFSVoRCW_2dFZwOypRA";
        String principalPassword = "OP0NNVBFLPlTzmu/+zrTvPg4q0dk+3d3g1bg1MJ4/CU3R4uRs7059zIASNAtMvs9RQ==";
        System.out.println("principalPassword : " + principalPassword);
        String algorithm = "AWS4-HMAC-SHA256";
        String region = "cn-shanghai-3";
        String service = "datacloud";
        String signedHeaders = "content-type;host;x-amz-date";
        String host = "datacloud.pocout.cloud.com";
        String method = "POST";
        String canonical_uri = "/";
        String canonical_querystring = "Action=dglogtest&Version=v1&AccountId=111";
        String content_type = "text/plain";
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date();
        String credentialData = df.format(date);
        String credential = principalkey + "/" + credentialData + "/" + region + "/" + service + "/aws4_request";
        String amzdate = TimeUtils.getWordTime(date);
        String payload = "[\"flag\"]";
        JSONObject params = new JSONObject();
        params.put("accessKey", principalkey);
        params.put("secret_key", principalPassword);
        params.put("credentialData", credentialData);
        params.put("region", region);
        params.put("service", service);
        params.put("credential", credential);
        params.put("amzdate", amzdate);
        params.put("signedHeaders", signedHeaders);
        params.put("method", method);
        params.put("canonical_uri", canonical_uri);
        params.put("canonical_querystring", canonical_querystring);
        params.put("host", host);
        params.put("content-type", content_type);
        params.put("algorithm", algorithm);
        params.put("payload", payload);
        params.put("comresssEanble", true);

        ByteArrayOutputStream originalContent = new ByteArrayOutputStream();
        originalContent.write(payload.getBytes(StandardCharsets.UTF_8));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(baos);
        originalContent.writeTo(gzipOutputStream);
        gzipOutputStream.finish();
        byte[] payloadByteArray = baos.toByteArray();

        JSONObject responseJson = null;

        URI hostUri = URI.create("http://datacloud.api.pocout.cloud/dglogtest?Action=dglogtest&Version=v1&AccountId=111&tenantId=24");
        Map<String, List<String>> parameters = Maps.newHashMap();
        parameters.put("Action", Lists.newArrayList("dglogtest"));
        parameters.put("Version", Lists.newArrayList("v1"));
        parameters.put("AccountId", Lists.newArrayList("111"));
        parameters.put("tenantId", Lists.newArrayList("24"));
        Map<String, String> headers = new HashMap<>();
        List<Header> headerList = NetworkAuthHeaderUtil.getAuthHeaderForJson(hostUri, parameters, payload, HttpMethod.POST, headers, service, region, principalkey, principalPassword);
        Map<String, String> headerParams = new HashMap<>();
        for (int i = 0; i < headerList.size(); i++) {
            headerParams.put(headerList.get(i).getName(), headerList.get(i).getValue());
        }
        headerParams.put("tenantId", "24");
        headerParams.put("projectId", "358");
        headerParams.put("env", "test");
        headerParams.put("topicName", "t930_channel_flow_json_source");
        headerParams.put("errTopicName", null);
        headerParams.put("ownerProjectId", "358");
        headerParams.put("errOwnerProjectId", null);
        headerParams.put("kafkaSourceId", "244");
        headerParams.put("appName", "111");
        headerParams.put("requestMethod", "API");
        headerParams.put("projectName", "linhb_930");
        headerParams.put("ip", "10.66.0.50");
        headerParams.put("appId", "276253");

        System.out.println(headerParams.toString());
        responseJson = DataSink.doHttpPost("true", "https://dg.bigdata.yun.ccb.com/dglogtest?Action=dglogtest&Version=v1&AccountId=111&tenantId=24",
                payload, payloadByteArray, headerParams, 5000, 5000, true);
        System.out.println(responseJson);

        String str = "123";
        System.out.println(AgentEncryption.decode("aksk2/6GX2JS-LW+D:nYGNdD'K\\HH%-%9b>K?MB6G^6a:,[XO>KE%i<Mb@=:C([g>c`AG<?CCe)$>$N)G\\22"));


    }

    public static String getRegexString(String regex, String str) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        String result = "";
        while (matcher.find()) {
            result = matcher.group();
        }
        return result;
    }
}
