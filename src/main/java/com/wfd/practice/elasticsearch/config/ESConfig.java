package com.wfd.practice.elasticsearch.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @ClassName ESConfig.java
 * @Description TODO
 * @Author 王凤铎
 * @Date 2020/7/10 9:46
 * @Version 1.0
 */
@Configuration
public class ESConfig {


    private String name;

    private String port;


    @Bean("getClient")
    public RestHighLevelClient getClient(){
        System.out.println("容器启动初始化...");
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("149.129.83.145", 9200, "http")));
        return client;
    }
}
