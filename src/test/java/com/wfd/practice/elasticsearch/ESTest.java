package com.wfd.practice.elasticsearch;


import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;


import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.Before;
import org.junit.Test;

import javax.swing.plaf.synth.SynthScrollBarUI;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author ：王凤铎
 * @date ：Created in 2020/6/23 9:34
 * @description：
 * @modified By：
 * @version: $
 */
public class ESTest {
    private RestHighLevelClient client;


    @Before
    public void connectES() throws Exception {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("149.129.83.145", 9200, "http"),
                        new HttpHost("149.129.83.145", 9300, "http")));
        System.out.println("连接成功！");
    }


    //创建索引
    @Test
    public void createIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("lamp-test");//创建索引
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println( JSON.toJSONString( createIndexResponse ) );

        //释放资源
        client.close();
    }


    //删除索引
    @Test
    public void deleteIndex() throws IOException {
//        //DeleteIndexRequest  request = new DeleteIndexRequest("twitter_two");
//        DeleteRequest request=new DeleteRequest("twitter_two","aTe4AjokRFGNTS6kz4jQtw");
//        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
//        //AcknowledgedResponse deleteResponse = client.indices().delete(request,RequestOptions.DEFAULT);
//        //删除索引响应结果
//        System.out.println( deleteResponse.getResult() );
        //删除索引请求对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest( "twitter_two" );
        //删除索引
        AcknowledgedResponse deleteIndexResponse = client.indices().delete( deleteIndexRequest, RequestOptions.DEFAULT);
        //删除索引响应结果
        boolean acknowledged = deleteIndexResponse.isAcknowledged();
        System.out.println( acknowledged );


    }

    //创建文档（document）
    @Test
    public void addDocument() throws IOException {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "王凤铎");
        jsonMap.put("age", 25);
        jsonMap.put("message", "王傻子哈哈笑");
        //jsonMap.put("email", "wangfd0820@126.com");

        IndexRequest indexRequest = new IndexRequest("lamp-test").id("44").source(jsonMap);
        //索引响应对象
        IndexResponse index = client.index( indexRequest,RequestOptions.DEFAULT );

        //获取响应结果
        DocWriteResponse.Result result = index.getResult();
        System.out.println( result );

    }


    //更新文档
    @Test
    public void updateDocument() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put( "user", "王傻子" );
        //map.put("message","hahaha elasticsearch!!!");
        UpdateRequest request = new UpdateRequest("lamp-test", "44").doc(map);

        UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
        RestStatus status=updateResponse.status();
        System.out.println("响应结果："+status);

    }

    //获取文档
    @Test
    public void getDocument() throws IOException {
        //根据ID查询
        GetRequest request = new GetRequest("lamp-test", "11");
        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
        //获取整个对象
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        System.out.println( sourceAsMap );
    }

    //删除文档
    @Test
    public void deleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("lamp-test", "33");

        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
        System.out.println( deleteResponse.status() );
    }

    //查询所有
    @Test
    public void searchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest("lamp-test");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //source源字段过滤
        //设置需要返回哪些具体的字段
        searchSourceBuilder.fetchSource( new String[]{"user", "name", "email"}, new String[]{} );

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //要访问返回的文档，我们需要首先获取SearchHits 响应中包含的内容
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits().value;
        System.out.println( "总共：" + totalHits );
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit
            String index = hit.getIndex();
            String id = hit.getId();
            float score = hit.getScore();
            System.out.println( "结果(index-id-score)：" + index+"----"+id+"----"+score );
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            //String articleId = String.valueOf( sourceAsMap.get( "id" ) );
            String user = (String) sourceAsMap.get( "user" );
            String message = (String) sourceAsMap.get( "message" );
            String email = (String) sourceAsMap.get( "email" );
            //System.out.println("articleId="+articleId);
            System.out.println( "用户：" + user );
            System.out.println( "信息：" + message );
            System.out.println( "邮箱：" + email );

        }

    }


    //相关度查询使用match，精确字段查询使用matchPhrase即可
    // 精准匹配的termQuery查不到数据（ES rest client的bug）换用matchPhraseQuery替换

    //term_query(一致完全满足)
    //Term Query精确查找 ，在搜索时会整体匹配关键字，不再将关键字分词
    @Test
    public void TestTermSearch() throws IOException {
        SearchRequest searchRequest=new SearchRequest("lamp-test");
        //设置查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query( QueryBuilders.matchPhraseQuery("message","哈哈笑") );
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //source源字段过虑
        //searchSourceBuilder.fetchSource(new String[]{"title","id","content"},new String[]{});
        searchSourceBuilder.fetchSource( new String[]{"user", "message", "email"}, new String[]{} );
        searchRequest.source( searchSourceBuilder );

        //查询响应
        SearchResponse search = client.search( searchRequest ,RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        long totalHits = hits.getTotalHits().value;
        System.out.println( "总条数：" + totalHits );

        for (SearchHit searchHit : hits.getHits()) {
            String sourceAsString = searchHit.getSourceAsString();
            System.out.println( sourceAsString );
        }



    }


    //match_query(词项满足)
    //搜索管理 match query 先分词后查找 minimum_should_match
    @Test
    public void TestMatchQuery() throws IOException {
        SearchRequest searchRequest=new SearchRequest("lamp-test");
        //设置查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("message","王凤铎哈哈大笑"));
        searchSourceBuilder.fetchSource( new String[]{"user", "message", "email"}, new String[]{} );
        HighlightBuilder highlightBuilder = new HighlightBuilder();
//        HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field("message");
//        highlightTitle.highlighterType("unified");
//        highlightBuilder.field(highlightTitle);
        HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
        highlightBuilder.field(highlightUser);
        searchSourceBuilder.highlighter(highlightBuilder);


        searchRequest.source( searchSourceBuilder );

        //查询响应
        SearchResponse search = client.search( searchRequest ,RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        long totalHits = hits.getTotalHits().value;
        System.out.println( "总条数：" + totalHits );

        for (SearchHit searchHit : hits.getHits()) {
            String sourceAsString = searchHit.getSourceAsString();
            System.out.println( sourceAsString );
        }
    }


    //通过多个ID获取（可存为数组）
    @Test
    public void MultiGet() throws IOException {
        MultiGetRequest request = new MultiGetRequest();
        request.add(new MultiGetRequest.Item("lamp-test", "22"));
        request.add(new MultiGetRequest.Item("lamp-test", "11"));

        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
        MultiGetItemResponse firstItem = response.getResponses()[0];

        GetResponse firstGet = firstItem.getResponse();
        String index = firstItem.getIndex();
        String id = firstItem.getId();
        if (firstGet.isExists()) {
            long version = firstGet.getVersion();
            String sourceAsString = firstGet.getSourceAsString();
            Map<String, Object> sourceAsMap = firstGet.getSourceAsMap();
            //byte[] sourceAsBytes = firstGet.getSourceAsBytes();
            System.out.println(sourceAsString);
        }
        System.out.println(index+"、"+id);

        //第二条数据（若为多个，数组循环遍历）
        MultiGetItemResponse secondItem=response.getResponses()[1];
        GetResponse secondGet=secondItem.getResponse();
        if (secondGet.isExists()) {
            String sourceString = secondGet.getSourceAsString();
            Map<String, Object> sourceMap = secondGet.getSourceAsMap();
            //byte[] sourceAsBytes = firstGet.getSourceAsBytes();
            System.out.println(sourceString);
        }
    }



}
