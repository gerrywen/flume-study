package com.ksyun.dc.flume.utils;

import net.sf.json.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * program: flume-study->HttpRequest
 * description: http请求工具
 * author: gerry
 * created: 2020-04-20 10:00
 **/
public class HttpRequest {

    // 日志打印
    private static final Logger logger = LoggerFactory.getLogger(HttpRequest.class);

    /**
     * GET请求方式
     *
     * @param urladdress
     * @param param
     * @param headparams
     * @param readTimeout
     * @param conTimeout
     * @return
     */
    public static String doRequest(String urladdress, String param, Map<String, String> headparams,
                                   int readTimeout, int conTimeout) {
        try {
            String urlStr = "" + urladdress + "?" + param;
            URL url = new URL(urlStr);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("GET");
            connection.setUseCaches(false);
            connection.setInstanceFollowRedirects(true);
            connection.setRequestProperty("Content-Type", "text/plain");
            for (String sb : headparams.keySet()) {
                connection.setRequestProperty(sb, headparams.get(sb));
            }
            connection.setConnectTimeout(conTimeout);
            connection.setReadTimeout(readTimeout);
            connection.connect();
            DataOutputStream out = new DataOutputStream(connection.getOutputStream());
//            Map<String, Object> obj = new HashMap<String, Object>();
//            obj.put("user_id", "259");
//            obj.put("channel_id", "1101");
//            obj.put("product_id","2");
//            obj.put("starttime","20151008");
//            obj.put("endtime","20151009");
//            obj.put("d_step","288");
//            obj.put("sign","xxxxxxxx");
//            out.writeBytes("");
            out.flush();
            out.close();
//            JSONObject responseJson = new JSONObject();
//            int status = connection.getResponseCode();
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String lines;
            StringBuffer sb = new StringBuffer("");
            while ((lines = reader.readLine()) != null) {
                lines = new String(lines.getBytes(), StandardCharsets.UTF_8);
                sb.append(lines);
            }
            reader.close();
            connection.disconnect();
            return sb.toString();
        } catch (MalformedURLException e) {
            e.printStackTrace();
            logger.error("HttpRequest doRequest MalformedURLException {}: ", e.getMessage());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            logger.error("HttpRequest doRequest UnsupportedEncodingException {}: ", e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("HttpRequest doRequest IOException {}: ", e.getMessage());
        }

        return "";
    }

    public static JSONObject doPostGZIP(String urladdress, byte[] payloadByteArray,
                                        Map<String, String> headparams, int readTimeout, int conTimeout) {
        BufferedReader reader = null;
        HttpURLConnection connection = null;
        InputStreamReader inputStream = null;
        JSONObject responseJson = new JSONObject();
        try {
            URL url = new URL(urladdress);
            connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setUseCaches(false);
            connection.setInstanceFollowRedirects(true);
            for (String sb : headparams.keySet()) {
                connection.setRequestProperty(sb, headparams.get(sb));
            }
            connection.setConnectTimeout(conTimeout);
            connection.setReadTimeout(readTimeout);
            connection.connect();
            DataOutputStream outs = new DataOutputStream(connection.getOutputStream());
            // 将参数写入流，刷新提交关闭流
            outs.write(payloadByteArray);
            outs.flush();
            outs.close();

            InputStream stream = connection.getInputStream();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int i;
            while ((i = stream.read()) != -1) {
                baos.write(i);
            }
            String str = baos.toString();
            logger.info(str);

            inputStream = new InputStreamReader(connection.getInputStream());
            reader = new BufferedReader(inputStream);
            int statusCode = connection.getResponseCode();

            String lines;
            StringBuffer sb = new StringBuffer("");
            while ((lines = reader.readLine()) != null) {
                lines = new String(lines.getBytes(), StandardCharsets.UTF_8);
                sb.append(lines);
            }
            responseJson.put("content", sb.toString());
            responseJson.put("code", statusCode);

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 500);
            responseJson.put("errorCode", 6001);
            e.printStackTrace();
        } catch (SocketTimeoutException | ConnectException | UnsupportedEncodingException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 408);
            e.printStackTrace();
        } catch (IOException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 400);
            e.printStackTrace();
        } finally {
            try {
                if (connection != null) {
                    connection.disconnect();
                }
                if (inputStream != null) {
                    inputStream.close();
                }
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return responseJson;
    }


    /**
     * post 请求
     *
     * @param urladdress
     * @param postJsonparams
     * @param headparams
     * @param readTimeout
     * @param conTimeout
     * @return
     */
    public static JSONObject doPost(String urladdress, String postJsonparams, Map<String, String> headparams,
                                    int readTimeout, int conTimeout) {
        PrintWriter out = null;
        BufferedReader reader = null;
        HttpURLConnection connection = null;
        JSONObject responseJson = new JSONObject();
        try {
            URL url = new URL(urladdress);
            connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("POST");
            connection.setUseCaches(false);
            connection.setInstanceFollowRedirects(true);
            connection.setRequestProperty("Content-Type", "text/plain");
            for (String sb : headparams.keySet()) {
                connection.setRequestProperty(sb, headparams.get(sb));
            }
            connection.setConnectTimeout(conTimeout);
            connection.setReadTimeout(readTimeout);
            connection.connect();
            out = new PrintWriter(connection.getOutputStream());
            // 发送请求参数
            out.print(postJsonparams);
            // flush输出流的缓冲
            out.flush();
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            int statusCode = connection.getResponseCode();
            String lines;
            StringBuffer sb = new StringBuffer("");
            while ((lines = reader.readLine()) != null) {
                lines = new String(lines.getBytes(), StandardCharsets.UTF_8);
                sb.append(lines);
            }
            reader.close();
            connection.disconnect();
            responseJson.put("content", sb.toString());
            responseJson.put("code", statusCode);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 500);
            e.printStackTrace();
        } catch (SocketTimeoutException | ConnectException | UnsupportedEncodingException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 408);
            e.printStackTrace();
        } catch (IOException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 400);
            e.printStackTrace();
        } finally {
            if (out != null) {
                out.close();
            }
        }
        return responseJson;
    }

    public static JSONObject doPost2(String urladdress, byte[] payloadByteArray, Map<String, String> headparams,
                                     int readTimeout, int conTimeout) {
        JSONObject responseJson = new JSONObject();
        CloseableHttpClient httpClient = HttpClients.createDefault();
        // 创建httpPost
        HttpPost httpPost = new HttpPost(urladdress);
        CloseableHttpResponse response = null;
        ByteArrayEntity arrayEntity = new ByteArrayEntity(payloadByteArray);
        arrayEntity.setContentType("application/octet-steam");
        httpPost.setEntity(arrayEntity);
        for (String sb : headparams.keySet()) {
            httpPost.setHeader(sb, headparams.get(sb));
        }

        try {

            response = httpClient.execute(httpPost);
            StatusLine statusLine = response.getStatusLine();
            int status = statusLine.getStatusCode();
            if (status == HttpStatus.SC_OK) {
                HttpEntity responseEntity = response.getEntity();
                String jsonString = EntityUtils.toString(responseEntity);
                logger.info("responseEntity >>> " + jsonString);
                responseJson.put("code", 200);
                return responseJson;
            } else {
                HttpEntity responseEntity = response.getEntity();
                String jsonString = EntityUtils.toString(responseEntity);
                logger.info("responseEntity >>> " + jsonString);
                responseJson.put("content", jsonString);
                responseJson.put("code", 500);
                return null;
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 500);
            e.printStackTrace();
        } catch (SocketTimeoutException | ConnectException | UnsupportedEncodingException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 408);
            e.printStackTrace();
        } catch (IOException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 400);
            e.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return responseJson;
    }


    public static JSONObject doPostHttps(String urladdress, String postJsonparams, Map<String, String> headparams,
                                         int readTimeout, int conTimeout) {
        JSONObject responseJson = new JSONObject();
        CloseableHttpClient httpClient = HttpClientFactory.getHttpsClient();
        // 创建httpPost
        HttpPost httpPost = new HttpPost(urladdress);
        CloseableHttpResponse response = null;
        StringEntity entity = new StringEntity(postJsonparams, "UTF-8");
        httpPost.setEntity(entity);
        for (String sb : headparams.keySet()) {
            httpPost.setHeader(sb, headparams.get(sb));
        }

        try {

            response = httpClient.execute(httpPost);
            StatusLine statusLine = response.getStatusLine();
            int status = statusLine.getStatusCode();
            if (status == HttpStatus.SC_OK) {
                HttpEntity responseEntity = response.getEntity();
                String jsonString = EntityUtils.toString(responseEntity);
                logger.info("responseEntity >>> " + jsonString);
                responseJson.put("code", 200);
                return responseJson;
            } else {
                HttpEntity responseEntity = response.getEntity();
                String jsonString = EntityUtils.toString(responseEntity);
                logger.info("responseEntity >>> " + jsonString);
                return null;
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 500);
            e.printStackTrace();
        } catch (SocketTimeoutException | ConnectException | UnsupportedEncodingException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 408);
            e.printStackTrace();
        } catch (IOException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 400);
            e.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return responseJson;
    }


    public static JSONObject doPostHttps(String urladdress, byte[] payloadByteArray, Map<String, String> headparams,
                                         int readTimeout, int conTimeout) {
        JSONObject responseJson = new JSONObject();
        CloseableHttpClient httpClient = HttpClientFactory.getHttpsClient();
        // 创建httpPost
        HttpPost httpPost = new HttpPost(urladdress);
        CloseableHttpResponse response = null;
        ByteArrayEntity arrayEntity = new ByteArrayEntity(payloadByteArray);
        arrayEntity.setContentType("application/octet-steam");
        httpPost.setEntity(arrayEntity);
        for (String sb : headparams.keySet()) {
            httpPost.setHeader(sb, headparams.get(sb));
        }

        try {

            response = httpClient.execute(httpPost);
            StatusLine statusLine = response.getStatusLine();
            int status = statusLine.getStatusCode();
            if (status == HttpStatus.SC_OK) {
                HttpEntity responseEntity = response.getEntity();
                String jsonString = EntityUtils.toString(responseEntity);
                logger.info("responseEntity >>> " + jsonString);
                responseJson.put("content", jsonString);
                responseJson.put("code", 200);
                return responseJson;
            } else {
                HttpEntity responseEntity = response.getEntity();
                String jsonString = EntityUtils.toString(responseEntity);
                logger.info("responseEntity >>> " + jsonString);
                responseJson.put("content", jsonString);
                responseJson.put("code", status);
                return null;
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 500);
            e.printStackTrace();
        } catch (SocketTimeoutException | ConnectException | UnsupportedEncodingException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 408);
            e.printStackTrace();
        } catch (IOException e) {
            responseJson.put("content", e.toString());
            responseJson.put("code", 400);
            e.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        logger.info("responseJson>>>>>:" + responseJson);
        return responseJson;
    }


}
