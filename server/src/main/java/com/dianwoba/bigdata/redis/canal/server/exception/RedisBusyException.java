package com.dianwoba.bigdata.redis.canal.server.exception;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 17:22
 */
public class RedisBusyException extends RedisDataException {
    private static final long serialVersionUID = 3992655220229243478L;

    public RedisBusyException(String message) {
        super(message);
    }

    public RedisBusyException(Throwable cause) {
        super(cause);
    }

    public RedisBusyException(String message, Throwable cause) {
        super(message, cause);
    }
}
