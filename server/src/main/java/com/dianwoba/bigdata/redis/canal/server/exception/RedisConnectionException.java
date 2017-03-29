package com.dianwoba.bigdata.redis.canal.server.exception;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 17:11
 */
public class RedisConnectionException extends RedisException {
    private static final long serialVersionUID = 3878126572474819403L;

    public RedisConnectionException(String message) {
        super(message);
    }

    public RedisConnectionException(Throwable cause) {
        super(cause);
    }

    public RedisConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
