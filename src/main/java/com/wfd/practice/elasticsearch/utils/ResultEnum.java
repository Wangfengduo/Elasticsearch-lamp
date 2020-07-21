package com.wfd.practice.elasticsearch.utils;


import lombok.Getter;


@Getter
public enum ResultEnum {

    /**
     * 服务器出错
     */
    SERVER_ERROR(404, "服务器出错"),

    /**
     * 账户问题
     */
    USER_EXIST(401, "该用户名已经存在"),
    USER_PWD_NULL(402, "密码不能为空"),
    USER_ACT_PWD_ERROR(402, "账号或密码错误"),
    USER_INEQUALITY(403, "两次密码不一致"),
    USER_OLD_PWD_ERROR(404, "原来密码不正确"),
    USER_NAME_PWD_NULL(405, "用户名和密码不能为空"),
    USER_CAPTCHA_ERROR(406, "验证码错误"),

    /**
     * 非法操作
     */
    STATUS_ERROR(401, "非法操作：状态有误"),

    /**
     * 权限问题
     */
    NO_PERMISSIONS(401, "权限不足！"),
    NO_ADMIN_AUTH(500, "不允许操作超级管理员"),
    NO_ADMIN_STATUS(501, "不能修改超级管理员状态"),
    NO_ADMINROLE_AUTH(500, "不允许操作管理员角色"),
    NO_ADMINROLE_STATUS(501, "不能修改管理员角色状态"),

    /**
     * 文件操作
     */
    NO_FILE_NULL(401, "文件不能为空"),
    NO_FILE_TYPE(402, "不支持该文件类型"),

    /**
     * 数据请求
     */
    PARAMETER_ERROR(400, "参数错误"),

    /**
     * 数据返回结果
     */
    UNKNOWN_ERROR(-1,"未知错误"),
    SUCCESS(200, "成功"),
    ERROR(500, "错误")

    ;

    private Integer code;

    private String message;

    ResultEnum(Integer code, String message) {
        this.code = code;
        this.message = message;
    }


}