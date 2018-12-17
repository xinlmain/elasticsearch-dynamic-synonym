package com.ginobefunny.elasticsearch.plugins.synonym.service;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class RemoteSynonymRulesReader implements SynonymRulesReader {

    private static Logger logger = ESLoggerFactory.getLogger("dynamic-synonym");

    private CloseableHttpClient httpclient;

    /**
     * Remote URL address
     */
    private String location;

    private long currentMaxVersion;
    private long lastUpdateVersion;

    public RemoteSynonymRulesReader(String location) {
        this.location = location;

        this.httpclient = AccessController.doPrivileged((PrivilegedAction<CloseableHttpClient>) HttpClients::createDefault);

        // 第一次获取当前版本
        isNeedReloadSynonymRules();
    }

    @Override
    public boolean isNeedReloadSynonymRules() {
        RequestConfig rc = RequestConfig.custom()
            .setConnectionRequestTimeout(2 * 1000)
            .setConnectTimeout(2 * 1000).setSocketTimeout(4 * 1000)
            .build();
        HttpGet get = new HttpGet(location + "/version");
        get.setConfig(rc);

        BufferedReader br = null;
        CloseableHttpResponse response = null;
        try {
            response = executeHttpRequest(get);
            if (response.getStatusLine().getStatusCode() == 200) { // 返回200 才做操作
                Reader reader = new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8);
                br = new BufferedReader(reader);
                String result = br.readLine();
                currentMaxVersion = Long.parseLong(result);

                if (lastUpdateVersion < currentMaxVersion) {
                    logger.info("remote synonym version is: {}, local version is: {}, need to reload.", currentMaxVersion, lastUpdateVersion);
                    return true;
                }
                return false;

            } else {
                logger.info("remote synonym {} return bad code {}", location,
                    response.getStatusLine().getStatusCode());
            }

        } catch (IOException e) {
            logger.error("check need reload remote synonym {} error!", e,
                location);
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                logger.error("failed to close http response", e);
            }
        }
        return false;
    }

    @Override
    public String reloadSynonymRules() {
        StringBuilder sb = new StringBuilder();

        RequestConfig rc = RequestConfig.custom()
            .setConnectionRequestTimeout(10 * 1000)
            .setConnectTimeout(10 * 1000).setSocketTimeout(30 * 1000)
            .build();
        CloseableHttpResponse response;
        BufferedReader br = null;
        HttpGet get = new HttpGet(location + "/text");
        get.setConfig(rc);
        try {
            response = executeHttpRequest(get);
            if (response.getStatusLine().getStatusCode() == 200) {
                String charset = "UTF-8"; // 获取编码，默认为utf-8
                if (response.getEntity().getContentType().getValue()
                    .contains("charset=")) {
                    String contentType = response.getEntity().getContentType()
                        .getValue();
                    charset = contentType.substring(contentType
                        .lastIndexOf('=') + 1);
                }

                br = new BufferedReader(new InputStreamReader(response
                    .getEntity().getContent(), charset));

                String line;
                while ((line = br.readLine()) != null) {
                    logger.info("reloading remote synonym: {}", line);
                    sb.append(line)
                        .append(System.getProperty("line.separator"));
                }

                //TODO: here we assume the reloading is always successful, which is not.
                lastUpdateVersion = currentMaxVersion;
            }
        } catch (Exception e) {
            logger.error("get remote synonym reader {} error!", e, location);
            throw new IllegalArgumentException(
                "Exception while reading remote synonyms file", e);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                logger.error("failed to close bufferedReader", e);
            }
        }

        return sb.toString();
    }

    private CloseableHttpResponse executeHttpRequest(HttpUriRequest httpUriRequest) {
        return AccessController.doPrivileged((PrivilegedAction<CloseableHttpResponse>) () -> {
            try {
                return httpclient.execute(httpUriRequest);
            } catch (IOException e) {
                logger.error("Unable to execute HTTP request: {}", e);
            }
            return null;
        });
    }
}
