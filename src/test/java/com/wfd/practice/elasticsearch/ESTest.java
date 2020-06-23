package com.wfd.practice.elasticsearch;


import org.apache.http.HttpHost;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Before;
import org.junit.Test;

/**
 * @author ：王凤铎
 * @date ：Created in 2020/6/23 9:34
 * @description：
 * @modified By：
 * @version: $
 */
public class ESTest {

    @Before
    public void connectES(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("149.129.83.145", 9200, "http"),
                        new HttpHost("149.129.83.145", 9201, "http")));
    }


    @Test
    public void createIndex(){

    }



}
