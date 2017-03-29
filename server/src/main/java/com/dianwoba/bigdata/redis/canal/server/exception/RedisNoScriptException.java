package com.dianwoba.bigdata.redis.canal.server.exception;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 17:24
 */
public class RedisNoScriptException extends RedisDataException {
    private static final long serialVersionUID = 4674378093072060731L;

    public RedisNoScriptException(String message) {
        super(message);
    }

    public RedisNoScriptException(Throwable cause) {
        super(cause);
    }

    public RedisNoScriptException(String message, Throwable cause) {
        super(message, cause);
    }
}
