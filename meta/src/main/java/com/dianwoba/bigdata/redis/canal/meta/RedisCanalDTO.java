package com.dianwoba.bigdata.redis.canal.meta;

import java.io.Serializable;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 19:34
 */
public class RedisCanalDTO implements Serializable {

    public RedisCanalDTO() {
    }

    public RedisCanalDTO(String key, String value, Long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }



    private String key;
    private String value;
    private Long timestamp;

    public String toString() {
        return String.format("{\"key\":\"%s\",\"value\":\"%s\",\"timestamp\":%s}", key, value, timestamp);
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public static void main(String[] args) {
        System.out.println(new RedisCanalDTO("aa", "aa", 1L).toString());
    }

}
