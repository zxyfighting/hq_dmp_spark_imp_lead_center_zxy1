package com.faw.hq.dmp.spark.imp.thread.util;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class HttpClientExtendUtil {

    public static String doPostRequestBody(String url, String requestBody) {
        // 创建Httpclient对象
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        String resultString = "";
        Logger logger = LoggerFactory.getLogger("scfl");
        try {
            // 创建Http Post请求
            HttpPost httpPost = new HttpPost(url);
            StringEntity entityBody = new StringEntity(requestBody, "UTF-8");
            entityBody.setContentType("application/json");
            httpPost.setEntity(entityBody);
            response = httpClient.execute(httpPost);
            resultString = EntityUtils.toString(response.getEntity(), "utf-8");
            logger.info(resultString);
        } catch (Exception e) {
            logger.error("--------httpError-----"+"e:",e);
        } finally {
            try {
                if(response!=null) {
                    response.close();
                }
            } catch (IOException e) {
                logger.error("--------IOException-----"+"e:",e);
            }
        }
        return resultString;
    }

}
