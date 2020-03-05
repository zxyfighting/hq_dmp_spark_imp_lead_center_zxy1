package com.faw.hq.dmp.spark.imp.thread.bean;


import java.io.Serializable;

public class Result implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = -1898897170796267714L;

    /**
     * 响应编码
     */
    private Integer code;
    /**
     * 响应业务状态名称
     */
    private String status;
    /**
     * 响应消息
     */
    private String msg;
    /**
     * 响应中的数据
     */
    private Object data;

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

}

