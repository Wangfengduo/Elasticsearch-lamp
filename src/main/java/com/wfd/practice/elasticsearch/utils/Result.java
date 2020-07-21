package com.wfd.practice.elasticsearch.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class Result {

    // 定义jackson对象
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** 状态码 **/
    private Integer code;

    /** 提示信息 **/
    private String msg;

    /** 返回的数据总数 **/
    private Long count;

    /** 响应数据 **/
    private Object data;

    public Result() {}

    public Result(Integer code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public Result(Integer code, String msg, Object data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }

    public Result(Integer code, String msg, Long count, Object data) {
        this.code = code;
        this.msg = msg;
        this.count = count;
        this.data = data;
    }



    public static Result ok() {
        return new Result(ResultEnum.SUCCESS.getCode(), ResultEnum.SUCCESS.getMessage());
    }

    public static Result ok(Object data) {
        return new Result(ResultEnum.SUCCESS.getCode(), ResultEnum.SUCCESS.getMessage(), data);
    }

    public static Result ok(Long count, Object data) {
        return new Result(ResultEnum.SUCCESS.getCode(), ResultEnum.SUCCESS.getMessage(), count, data);
    }

    public static Result error(){
        return new Result(ResultEnum.ERROR.getCode(), ResultEnum.ERROR.getMessage(), null);
    }

    public static Result error(String msg){
        return error(ResultEnum.ERROR.getCode(), msg);
    }

    public static Result error(Integer code, String msg){
        return new Result(code, msg, null);
    }

    public static Result build(Integer code, String msg) {
        return new Result(code, msg);
    }

    public static Result build(Integer code, String msg, Object data) {
        return new Result(code, msg, data);
    }

    public static Result build(Integer code, String msg, Long count, Object data) {
        return new Result(code, msg, count, data);
    }



    /**
     * 将json结果集转化为TaotaoResult对象
     *
     * @param jsonData json数据
     * @param clazz TaotaoResult中的object类型
     * @return
     */
    public static Result formatToPojo(String jsonData, Class<?> clazz) {
        try {
            if (clazz == null) {
                return MAPPER.readValue(jsonData, Result.class);
            }
            JsonNode jsonNode = MAPPER.readTree(jsonData);
            JsonNode data = jsonNode.get("data");
            Object obj = null;
            if (clazz != null) {
                if (data.isObject()) {
                    obj = MAPPER.readValue(data.traverse(), clazz);
                } else if (data.isTextual()) {
                    obj = MAPPER.readValue(data.asText(), clazz);
                }
            }
            return build(jsonNode.get("status").intValue(), jsonNode.get("msg").asText(), obj);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 没有object对象的转化
     *
     * @param json
     * @return
     */
    public static Result format(String json) {
        try {
            return MAPPER.readValue(json, Result.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Object是集合转化
     *
     * @param jsonData json数据
     * @param clazz 集合中的类型
     * @return
     */
    public static Result formatToList(String jsonData, Class<?> clazz) {
        try {
            JsonNode jsonNode = MAPPER.readTree(jsonData);
            JsonNode data = jsonNode.get("data");
            Object obj = null;
            if (data.isArray() && data.size() > 0) {
                obj = MAPPER.readValue(data.traverse(),
                        MAPPER.getTypeFactory().constructCollectionType(List.class, clazz));
            }
            return build(jsonNode.get("status").intValue(), jsonNode.get("msg").asText(), obj);
        } catch (Exception e) {
            return null;
        }
    }


    public static Result judge(int rows){
        return rows > 0 ? ok("操作成功！") : error("操作失败！");
    }

    public static Result judge(Object object){
        if(object == null){
            return Result.error();
        } else {
            return Result.ok(object);
        }
    }

    public static Result judge(Boolean b){
        if(b){
            return Result.ok();
        } else {
            return Result.error();
        }
    }


}
