package com.dianwoba.bigdata.redis.canal.server.exception;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 17:15
 */
public class RedisDataException extends RedisException {
    private static final long serialVersionUID = 3878126572474819403L;

    public RedisDataException(String message) {
        super(message);
    }

    public RedisDataException(Throwable cause) {
        super(cause);
    }

    public RedisDataException(String message, Throwable cause) {
        super(message, cause);
    }
}
