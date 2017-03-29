package com.dianwoba.bigdata.redis.canal.server.exception;

import com.dianwoba.bigdata.redis.canal.server.HostAndPort;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 17:20
 */
public class RedisAskDataException extends RedisRedirectionException{

    private static final long serialVersionUID = 3878126572474819403L;

    public RedisAskDataException(Throwable cause, HostAndPort targetHost, int slot) {
        super(cause, targetHost, slot);
    }

    public RedisAskDataException(String message, Throwable cause, HostAndPort targetHost, int slot) {
        super(message, cause, targetHost, slot);
    }

    public RedisAskDataException(String message, HostAndPort targetHost, int slot) {
        super(message, targetHost, slot);
    }

}
