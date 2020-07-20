package com.wfd.practice.elasticsearch.controller;

import com.wfd.practice.elasticsearch.utils.ESUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Map;

/**
 * @ClassName ESController.java
 * @Description TODO
 * @Author 王凤铎
 * @Date 2020/7/20 16:30
 * @Version 1.0
 */
@Controller
public class ESController {
    @Autowired
    private ESUtils esUtils;

    @RequestMapping(value = "/ES/selectById/{index}/{id}")
    public Map<String, Object> selectById(@PathVariable("index") String index, @PathVariable("id") String id){
        Map<String, Object> resultMap= esUtils.searchDataById(index,id);
        return resultMap;
    }


}
