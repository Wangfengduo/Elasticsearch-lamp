package com.wfd.practice.elasticsearch.utils;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @ClassName ESUtils.java
 * @Description TODO
 * @Author 王凤铎
 * @Date 2020/7/10 10:09
 * @Version 1.0
 */
@Component
public class ESUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ESUtils.class);

    @Autowired
    @Qualifier("getClient")
    private  RestHighLevelClient restHighLevelClient;


    static{
        System.out.println("static静态变量...");
    }
    /**
     * 创建索引
     * @param index
     * @return
     * @throws IOException
     */
    public  boolean createIndex(String index) throws IOException {

        if(!isIndexExist(index)){
            LOGGER.info("Index is not exits!");
        }
        CreateIndexRequest request = new CreateIndexRequest(index);//创建索引
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        LOGGER.info("执行建立成功？" + createIndexResponse.isAcknowledged());
        return createIndexResponse.isAcknowledged();
    }

    /**
     * 删除索引
     * @param index
     * @return
     * @throws IOException
     */
    public  boolean deleteIndex(String index) throws IOException {
        if(!isIndexExist(index)) {
            LOGGER.info("Index is not exits!");
        }
        //删除索引请求对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest( "twitter_two" );
        //删除索引
        AcknowledgedResponse deleteIndexResponse = restHighLevelClient.indices().delete( deleteIndexRequest, RequestOptions.DEFAULT);
        if (deleteIndexResponse.isAcknowledged()) {
            LOGGER.info("delete index " + index + "  successfully!");
        } else {
            LOGGER.info("Fail to delete index " + index);
        }
        return deleteIndexResponse.isAcknowledged();

    }


    /**
     * 添加数据 --指定ID
     * @param jsonObject
     * @param index
     * @param id
     * @return
     */
    public  String addData(JSONObject jsonObject, String index, String id) throws IOException {
        IndexRequest indexRequest = new IndexRequest(index).id(id).source(jsonObject);
        IndexResponse indexResponse = restHighLevelClient.index( indexRequest,RequestOptions.DEFAULT );
        LOGGER.info("addData response status:{},id:{}", indexResponse.status().getStatus(), indexResponse.getId());
        return indexResponse.getId();

    }


    /**
     * 添加数据 --随机ID生成
     * @param jsonObject
     * @param index
     * @return
     * @throws IOException
     */
    public  String addData(JSONObject jsonObject, String index) throws IOException {
        return addData(jsonObject, index, UUID.randomUUID().toString().replaceAll("-", "").toUpperCase());
    }


    /**
     * 通过ID删除数据
     * @param index
     * @param id
     */
    public  void deleteDataById(String index,String id) {
        DeleteRequest request = new DeleteRequest(index, id);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
            LOGGER.info("deleteDataById response status:{},id:{}", deleteResponse.status().getStatus(), deleteResponse.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据ID更新数据
     * @param jsonObject
     * @param index
     * @param id
     */
    public  void updateDataById(JSONObject jsonObject, String index, String id) {
        UpdateRequest request = new UpdateRequest(index, id).doc(jsonObject);

        try {
            UpdateResponse updateResponse = restHighLevelClient.update(request, RequestOptions.DEFAULT);
            LOGGER.info("updateDataById response status:{},id:{}", updateResponse.status().getStatus(), updateResponse.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 根据ID获取数据
     * @param index
     * @param id
     * @return
     */
    public  Map<String, Object> searchDataById(String index, String id) {
        GetRequest request = new GetRequest(index, id);
        GetResponse getResponse = null;
        try {
            getResponse = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //获取整个对象
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        LOGGER.info("searchResult:",sourceAsMap);
        return sourceAsMap;
    }



    /**
     * matchAll查询(不分页)
     * @param index
     * @return
     */
    public  List<Map<String, Object>> searchAllData(String index){
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //query => match_all
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        //返回结果
        SearchResponse search = null;
        try {
            search = restHighLevelClient.search( searchRequest , RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SearchHits hits = search.getHits();
        return Arrays.stream(hits.getHits()).map(b -> {
            return b.getSourceAsMap();
        }).collect(Collectors.toList());
    }


    /**
     * 搜索某一项
     * @param index 索引名称
     * @param field 查询字段名称
     * @param keyword 关键词
     * @param start 分页
     * @param size 分页条数
     * @return
     */
    public  List<Map<String, Object>> searchListData(String index,String field,String keyword,int start, int size){
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(field,keyword);
        searchSourceBuilder.query(matchQueryBuilder);
        searchSourceBuilder.from(start);
        searchSourceBuilder.size(size);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(searchSourceBuilder);

        SearchResponse search = null;
        try {
            search = restHighLevelClient.search( searchRequest ,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SearchHits hits = search.getHits();
        return Arrays.stream(hits.getHits()).map(b -> {
            return b.getSourceAsMap();
        }).collect(Collectors.toList());
    }






















    /**
     * 判断索引是否存在
     * @param index
     * @return
     */
    public  boolean isIndexExist(String index) {
        GetIndexRequest request = new GetIndexRequest(index);
        boolean exists = false;
        try {
            exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return exists;
    }


}
