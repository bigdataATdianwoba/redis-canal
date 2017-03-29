package com.dianwoba.bigdata.redis.canal.server.exception;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 17:22
 */
public class RedisClusterException extends RedisDataException{

    private static final long serialVersionUID = 3878126572474819403L;

    public RedisClusterException(Throwable cause) {
        super(cause);
    }

    public RedisClusterException(String message, Throwable cause) {
        super(message, cause);
    }

    public RedisClusterException(String message) {
        super(message);
    }

}
